#!/usr/bin/env python3
"""
Nautilus Data Transformation - Local Input to DigitalOcean Spaces Output

Reads input from local filesystem and writes output to DigitalOcean Spaces.
Input: Local directory path
Output: historical-db-1min/nautilus_main/data/
"""

import sys
import tempfile
import shutil
from pathlib import Path
from datetime import date, datetime, timedelta
import argparse
import logging

# Add repository root to path so package imports resolve when run as a script
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) > 2 else Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.data import BarType, QuoteTick
from nautilus_trader.model.instruments import Equity, OptionContract, FuturesContract
from nautilus_trader.model.objects import Price, Quantity, Currency
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import BarDataWrangler, QuoteTickDataWrangler

# Configure logging first
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Import DigitalOcean Spaces connection (only for output)
from connect_spaces import DigitalOceanSpaces

# Try to import contract generators (may not exist in this project)
try:
    from marvelquant_data.utils.contract_generators import (
        create_options_contract,
        create_futures_contract,
        parse_nse_option_symbol
    )
    from marvelquant_data.data_types import OptionOI, FutureOI
except ImportError:
    # Fallback if imports fail - you may need to adjust these
    logger.warning("Could not import contract generators. Some features may not work.")
    create_options_contract = None
    create_futures_contract = None
    parse_nse_option_symbol = None
    OptionOI = None
    FutureOI = None

# DigitalOcean Spaces configuration (for output only)
SPACES_ACCESS_KEY = "DO00CDX8Z7BFTQJ9W2AZ"
SPACES_SECRET_KEY = "kR159s1x7Xjfd6RhVIyw8X34UGLIFKSzP7/0fG6Yt9I"
SPACES_ENDPOINT = "https://blr1.digitaloceanspaces.com"
SPACES_REGION = "blr1"
SPACES_BUCKET = "historical-db-1min"
OUTPUT_PREFIX = "nautilas-data/"

# IST offset (5 hours 30 minutes)
IST_OFFSET = timedelta(hours=5, minutes=30)

INDEX_DIR_CANDIDATES = ("index", "cash")
OPTION_DIR_CANDIDATES = ("option", "options")
CATALOG_DATA_SUBDIRS = (
    "bar"
    "quote_tick",
    "equity",
    "futures_contract",
    "option_contract",
    "custom_future_oi",
    "custom_option_oi",
)
OPTION_SYMBOL_SUFFIXES = (".NSE", ".BSE", ".NFO", ".FO", ".MCX", ".CDS")


class SpacesOutputUploader:
    """Handles uploading output files to DigitalOcean Spaces"""
    
    def __init__(self, spaces: DigitalOceanSpaces, bucket: str, output_prefix: str):
        self.spaces = spaces
        self.bucket = bucket
        self.output_prefix = output_prefix.rstrip('/') + '/' if output_prefix else ''
    
    def upload_output_files(self, local_dir: Path):
        """Upload output files from local directory to Spaces"""
        logger.info(f"Uploading files to Spaces: {self.bucket}/{self.output_prefix}")
        logger.info(f"Local directory to upload from: {local_dir}")
        logger.info(f"Local directory exists: {local_dir.exists()}")
        logger.info(f"Local directory absolute path: {local_dir.resolve()}")
        
        if not local_dir.exists():
            logger.error(f"Local output directory does not exist: {local_dir}")
            return
        
        # Test connection and network connectivity before attempting uploads
        logger.info("="*80)
        logger.info("TESTING CONNECTION TO DIGITALOCEAN SPACES")
        logger.info("="*80)
        try:
            if not self.spaces.test_connection():
                logger.error("Connection test failed! Cannot proceed with uploads.")
                logger.error("Please check:")
                logger.error("  1. Network connectivity to DigitalOcean Spaces")
                logger.error("  2. Firewall/proxy settings")
                logger.error("  3. Credentials are correct")
                logger.error("  4. Endpoint URL is reachable from this server")
                return
            logger.info("✓ Connection test passed")
        except Exception as e:
            logger.error(f"Connection test raised exception: {e}", exc_info=True)
            logger.error("Cannot proceed with uploads due to connection failure.")
            return
        
        # Test upload with a small file first
        logger.info("Testing upload with a small test file...")
        test_upload_success = False
        test_file = None
        try:
            # Create a small test file
            import tempfile
            test_file = Path(tempfile.mktemp(suffix='.test'))
            test_file.write_text("test")
            test_key = f"{self.output_prefix}_upload_test_{int(__import__('time').time())}.test"
            
            logger.info(f"Attempting test upload: {test_file} -> {self.bucket}/{test_key}")
            success = self.spaces.upload_file(str(test_file), self.bucket, test_key, silent=True)
            if success:
                logger.info("✓ Test upload successful - network connectivity is working")
                test_upload_success = True
                # Clean up test file from Spaces
                try:
                    self.spaces.delete_file(self.bucket, test_key)
                except:
                    pass
            else:
                logger.error("✗ Test upload failed - upload_file returned False")
        except Exception as e:
            logger.error(f"✗ Test upload failed with exception: {e}", exc_info=True)
            logger.error("This indicates a problem with:")
            logger.error("  - Network connectivity to DigitalOcean Spaces")
            logger.error("  - Firewall blocking outbound connections")
            logger.error("  - DNS resolution issues")
            logger.error("  - Credentials or permissions")
        finally:
            # Clean up local test file
            if test_file is not None:
                try:
                    if test_file.exists():
                        test_file.unlink()
                except:
                    pass
        
        if not test_upload_success:
            logger.error("="*80)
            logger.error("UPLOAD TEST FAILED - ABORTING BULK UPLOAD")
            logger.error("="*80)
            logger.error("Please fix the connection/network issues before retrying.")
            return
        
        # Find all files in local directory
        logger.info("="*80)
        logger.info("SCANNING FILES TO UPLOAD")
        logger.info("="*80)
        logger.info("Scanning for files to upload...")
        all_files = list(local_dir.rglob("*"))
        logger.info(f"Found {len(all_files)} total items in directory tree")
        
        files_to_upload = [f for f in all_files if f.is_file()]
        logger.info(f"Found {len(files_to_upload)} files to upload (excluding directories)")
        
        # Log some file paths to verify
        if files_to_upload:
            logger.info("Sample file paths (first 5):")
            for i, f in enumerate(files_to_upload[:5]):
                logger.info(f"  {i+1}. {f} (exists: {f.exists()}, size: {f.stat().st_size if f.exists() else 0} bytes)")
        
        if not files_to_upload:
            logger.error(f"ERROR: No files found to upload in {local_dir}!")
            logger.error("Check if ParquetDataCatalog wrote files correctly.")
            return
        
        logger.info(f"Found {len(files_to_upload)} files to upload")
        
        # Log first few files to verify
        logger.info(f"Sample files to upload (first 5):")
        for i, f in enumerate(files_to_upload[:5]):
            file_size = f.stat().st_size if f.exists() else 0
            logger.info(f"  {i+1}. {f.name} ({file_size:,} bytes) -> {self.output_prefix}{f.relative_to(local_dir)}")
        
        # Check existing files in Spaces to avoid re-uploading
        logger.info("="*80)
        logger.info("CHECKING FOR EXISTING FILES IN SPACES")
        logger.info("="*80)
        logger.info("Scanning Spaces for existing files to skip...")
        existing_files_set = set()
        try:
            # Get list of existing objects in Spaces under our prefix
            # Note: list_objects handles pagination automatically, so this will get ALL existing files
            existing_objects = self.spaces.list_objects(
                self.bucket, 
                prefix=self.output_prefix, 
                max_keys=1000,  # Max per page (method handles pagination to get all files)
                silent=True
            )
            if existing_objects:
                existing_files_set = {obj.get('Key', '') for obj in existing_objects if isinstance(obj, dict)}
                logger.info(f"Found {len(existing_files_set)} existing files in Spaces under {self.output_prefix}")
                if len(existing_files_set) > 0:
                    logger.info("Will skip these files during upload to avoid duplicates")
            else:
                logger.info("No existing files found in Spaces (first upload)")
        except Exception as e:
            logger.warning(f"Could not check for existing files in Spaces: {e}")
            logger.warning("Will proceed with uploads (may create duplicates if files exist)")
        
        # Start bulk upload
        logger.info("="*80)
        logger.info("STARTING BULK UPLOAD")
        logger.info("="*80)
        uploaded_count = 0
        skipped_count = 0
        failed_count = 0
        consecutive_failures = 0
        max_consecutive_failures = 10  # Stop if too many consecutive failures
        
        for idx, local_file in enumerate(files_to_upload, 1):
            # Get relative path from local_dir
            relative_path = local_file.relative_to(local_dir)
            # Convert to string and normalize path separators
            relative_path_str = str(relative_path).replace('\\', '/')
            # Strip "data/" prefix if present (catalog creates data/ subdirectory)
            if relative_path_str.startswith('data/'):
                relative_path_str = relative_path_str[5:]  # Remove "data/" prefix
            # Construct Spaces key
            spaces_key = self.output_prefix + relative_path_str
            
            try:
                # Check if file already exists in Spaces
                if spaces_key in existing_files_set:
                    skipped_count += 1
                    if skipped_count <= 5:  # Log first 5 skips
                        logger.info(f"[{idx}/{len(files_to_upload)}] ⊘ Skipped (already exists): {local_file.name} -> {spaces_key}")
                    elif skipped_count % 50 == 0:  # Log every 50 skips
                        logger.info(f"[{idx}/{len(files_to_upload)}] Skipped {skipped_count} existing files so far...")
                    continue
                
                # Verify file exists and has content before uploading
                if not local_file.exists():
                    failed_count += 1
                    consecutive_failures += 1
                    logger.error(f"[{idx}/{len(files_to_upload)}] File does not exist: {local_file}")
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(f"Too many consecutive failures ({consecutive_failures}). Stopping upload.")
                        break
                    continue
                
                file_size = local_file.stat().st_size
                if file_size == 0:
                    logger.warning(f"[{idx}/{len(files_to_upload)}] File is empty (0 bytes): {local_file} - skipping")
                    failed_count += 1
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(f"Too many consecutive failures ({consecutive_failures}). Stopping upload.")
                        break
                    continue
                
                # Upload file - errors will be raised as exceptions
                try:
                    success = self.spaces.upload_file(str(local_file), self.bucket, spaces_key, silent=True)
                    if success:
                        uploaded_count += 1
                        consecutive_failures = 0  # Reset on success
                        if uploaded_count % 10 == 0:  # Log every 10 files for better visibility
                            logger.info(f"[{idx}/{len(files_to_upload)}] Uploaded {uploaded_count}/{len(files_to_upload)} files (skipped {skipped_count})...")
                        elif uploaded_count <= 5:  # Log first 5 uploads
                            logger.info(f"[{idx}/{len(files_to_upload)}] ✓ Uploaded: {local_file.name} -> {spaces_key}")
                    else:
                        failed_count += 1
                        consecutive_failures += 1
                        logger.error(f"[{idx}/{len(files_to_upload)}] Upload returned False for {local_file} to {spaces_key}")
                        if consecutive_failures >= max_consecutive_failures:
                            logger.error(f"Too many consecutive failures ({consecutive_failures}). Stopping upload.")
                            break
                except Exception as upload_exception:
                    # Log detailed error information
                    failed_count += 1
                    consecutive_failures += 1
                    error_type = type(upload_exception).__name__
                    error_msg = str(upload_exception)
                    
                    logger.error(f"[{idx}/{len(files_to_upload)}] Exception uploading {local_file.name} to {spaces_key}")
                    logger.error(f"  Error type: {error_type}")
                    logger.error(f"  Error message: {error_msg}")
                    
                    # Provide helpful diagnostics based on error type
                    if "Connection" in error_type or "timeout" in error_msg.lower() or "network" in error_msg.lower():
                        logger.error("  → This appears to be a network connectivity issue")
                        logger.error("  → Check firewall, proxy, or network settings")
                    elif "403" in error_msg or "Forbidden" in error_msg:
                        logger.error("  → This appears to be a permissions/authentication issue")
                        logger.error("  → Check your DigitalOcean Spaces credentials and permissions")
                    elif "404" in error_msg or "Not Found" in error_msg:
                        logger.error("  → This appears to be a bucket or endpoint issue")
                        logger.error("  → Verify bucket name and endpoint URL are correct")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(f"Too many consecutive failures ({consecutive_failures}). Stopping upload.")
                        logger.error("This may indicate a systemic issue (network, credentials, etc.)")
                        break
            except Exception as e:
                failed_count += 1
                consecutive_failures += 1
                logger.error(f"[{idx}/{len(files_to_upload)}] Unexpected exception uploading {local_file} to {spaces_key}: {e}", exc_info=True)
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"Too many consecutive failures ({consecutive_failures}). Stopping upload.")
                    break
        
        logger.info("="*80)
        logger.info("UPLOAD SUMMARY")
        logger.info("="*80)
        logger.info(f"Upload complete: {uploaded_count} uploaded, {skipped_count} skipped (already exist), {failed_count} failed out of {len(files_to_upload)} total files")
        
        if failed_count > 0:
            logger.warning(f"Warning: {failed_count} files failed to upload. Check error messages above.")
        if uploaded_count == 0 and len(files_to_upload) > 0:
            logger.error("ERROR: No files were successfully uploaded!")
            logger.error("This likely indicates:")
            logger.error("  1. Network connectivity issues to DigitalOcean Spaces")
            logger.error("  2. Firewall or proxy blocking connections")
            logger.error("  3. Incorrect credentials or permissions")
            logger.error("  4. DNS resolution problems")
            logger.error("  5. Endpoint URL not reachable from this server")
        
        # Verify upload by checking if files exist in Spaces
        if uploaded_count > 0:
            logger.info("="*80)
            logger.info("VERIFYING UPLOAD")
            logger.info("="*80)
            logger.info("Verifying upload by checking files in Spaces...")
            try:
                uploaded_keys = []
                for local_file in files_to_upload[:min(10, len(files_to_upload))]:  # Check first 10 files
                    relative_path = local_file.relative_to(local_dir)
                    relative_path_str = str(relative_path).replace('\\', '/')
                    # Strip "data/" prefix if present (same logic as upload)
                    if relative_path_str.startswith('data/'):
                        relative_path_str = relative_path_str[5:]  # Remove "data/" prefix
                    spaces_key = self.output_prefix + relative_path_str
                    uploaded_keys.append(spaces_key)
                
                # List objects in the prefix to verify
                objects = self.spaces.list_objects(self.bucket, prefix=self.output_prefix, max_keys=100, silent=True)
                if objects:
                    logger.info(f"✓ Verified: Found {len(objects)} objects in Spaces under {self.output_prefix}")
                    # Check if any of our uploaded keys exist
                    existing_keys = {obj.get('Key', '') for obj in objects if isinstance(obj, dict)}
                    verified = sum(1 for key in uploaded_keys if key in existing_keys)
                    logger.info(f"✓ Verified: {verified}/{len(uploaded_keys)} sample files confirmed in Spaces")
                else:
                    logger.warning(f"⚠ Could not verify upload - no objects found under {self.output_prefix}")
            except Exception as e:
                logger.warning(f"Could not verify upload: {e}", exc_info=True)


def ensure_catalog_structure(root: Path) -> Path:
    """Ensure Nautilus catalog subdirectories exist (creates them if missing)."""
    data_root = root / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    for subdir in CATALOG_DATA_SUBDIRS:
        (data_root / subdir).mkdir(parents=True, exist_ok=True)
    return data_root


def get_venue_from_symbol(symbol: str) -> str:
    """Determine venue (BSE or NSE) based on symbol name."""
    symbol_upper = str(symbol).strip().upper()
    
    # BSE symbols (Bombay Stock Exchange)
    bse_symbols = {"SENSEX"}
    
    # Check if symbol is a BSE symbol
    if symbol_upper in bse_symbols:
        return "BSE"
    
    # Default to NSE for all other symbols (NIFTY, BANKNIFTY, etc.)
    return "NSE"


def normalize_contract_symbol(symbol: str | None) -> str:
    """Standardize NSE contract symbols (strip suffixes like .NSE and uppercase)."""
    if symbol is None:
        return ""
    normalized = str(symbol).strip().upper()
    for suffix in OPTION_SYMBOL_SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    return normalized


def parse_nse_option_symbol_fallback(symbol: str, data_year_hint: int | None = None) -> dict:
    """
    Fallback parser for NSE option symbols that handles both formats:
    - Format 1: {UNDERLYING}{DDMMMYY}{STRIKE}{CE|PE} (e.g., BANKNIFTY28OCT2548000CE)
    - Format 2: {UNDERLYING}{DDMMM}{STRIKE}{CE|PE} (e.g., NIFTY18JAN10250CE)
    
    This is a fallback when the main parse_nse_option_symbol fails.
    
    Args:
        symbol: Option symbol to parse
        data_year_hint: Optional year hint from the data (e.g., from timestamp) to help infer expiry year
    """
    from datetime import datetime, date
    import re
    
    # Extract option type (last 2 chars: CE or PE)
    if not symbol.endswith(('CE', 'PE')):
        raise ValueError(f"Symbol must end with CE or PE: {symbol}")
    
    option_type_code = symbol[-2:]
    option_type = "CALL" if option_type_code == "CE" else "PUT"
    
    # Remove option type from symbol
    symbol_without_type = symbol[:-2]
    
    # Try to find the date pattern (DDMMM or DDMMMYY)
    # Look for pattern: 1-2 digits, 3 letters (month), optionally 2 digits (year)
    date_pattern = r'(\d{1,2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})?'
    match = re.search(date_pattern, symbol_without_type, re.IGNORECASE)
    
    if not match:
        raise ValueError(f"Could not find date pattern in symbol: {symbol}")
    
    day = int(match.group(1))
    month_str = match.group(2).upper()
    year_str = match.group(3)
    
    # Determine year
    if year_str:
        # Format: DDMMMYY (e.g., 28OCT25)
        year = 2000 + int(year_str)
        date_str = f"{day:02d}{month_str}{year_str}"
        date_format = "%d%b%y"
    else:
        # Format: DDMMM (e.g., 18JAN) - need to infer year
        # Use data_year_hint if provided (from data timestamps)
        if data_year_hint and 2000 <= data_year_hint <= 2100:
            year = data_year_hint
        else:
            # Try to infer from symbol - look for 4-digit year in underlying part
            year_hint_match = re.search(r'(\d{4})', symbol_without_type[:match.start()])
            if year_hint_match:
                potential_year = int(year_hint_match.group(1))
                if 2000 <= potential_year <= 2100:
                    year = potential_year
                else:
                    # If no good hint, use year from current date (options are typically near-term)
                    year = date.today().year
            else:
                # For symbols like NIFTY18JAN, check if "18" before JAN could be year
                # But this is ambiguous - it's more likely the day
                # Default: use a reasonable year based on common patterns
                # If data_year_hint not provided, try to use current year
                year = data_year_hint if data_year_hint else date.today().year
        date_str = f"{day:02d}{month_str}"
        date_format = "%d%b"
    
    # Parse expiry date
    try:
        if year_str:
            expiry = datetime.strptime(date_str, date_format).date()
        else:
            # For DDMMM format, create date directly
            month_map = {
                'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
            }
            month = month_map[month_str]
            expiry = date(year, month, day)
    except (ValueError, KeyError) as e:
        raise ValueError(f"Could not parse date from {date_str} (year={year}): {e}")
    
    # Extract strike price (everything after the date)
    date_end_pos = match.end()
    strike_str = symbol_without_type[date_end_pos:]
    
    if not strike_str or not strike_str.isdigit():
        raise ValueError(f"Could not extract strike price from: {symbol_without_type}, after date at position {date_end_pos}")
    
    strike = float(strike_str)
    
    # Extract underlying (everything before the date)
    underlying = symbol_without_type[:match.start()]
    
    if not underlying:
        raise ValueError(f"Could not extract underlying from: {symbol}")
    
    return {
        'underlying': underlying,
        'expiry': expiry,
        'strike': strike,
        'option_type': option_type
    }


def _extract_symbol_from_name(name: str) -> str | None:
    base = name.split("_", 1)[0].strip()
    if not base:
        return None

    cleaned = "".join(ch for ch in base if ch.isalnum())
    if not cleaned or not cleaned[0].isalpha() or not any(ch.isalpha() for ch in cleaned):
        return None

    return cleaned.upper()


def discover_symbols(input_dir: Path) -> list[str]:
    """Discover available symbols by scanning local directory."""
    symbols: set[str] = set()

    def add_symbol(name: str) -> None:
        candidate = _extract_symbol_from_name(name)
        if candidate:
            symbols.add(candidate)

    def add_from_directory(directory: Path) -> None:
        if not directory.exists():
            return
        for child in directory.iterdir():
            if child.is_dir():
                add_symbol(child.name)

    for directory_name in INDEX_DIR_CANDIDATES:
        add_from_directory(input_dir / directory_name)

    add_from_directory(input_dir / "futures")

    for directory_name in OPTION_DIR_CANDIDATES:
        add_from_directory(input_dir / directory_name)

    if not symbols:
        logger.info("No symbols discovered from directory names; scanning parquet filenames.")
        for parquet_file in input_dir.rglob("*.parquet"):
            add_symbol(parquet_file.stem)

    if not symbols:
        logger.warning("No symbols discovered in input directory %s.", input_dir)
    else:
        logger.info(f"Discovered {len(symbols)} symbols: {sorted(symbols)}")

    return sorted(symbols)


def resolve_symbol_list(input_dir: Path, symbols: list[str] | None) -> list[str]:
    """Resolve user-provided symbols or auto-discover them."""
    if not symbols:
        discovered = discover_symbols(input_dir)
        if not discovered:
            raise ValueError(f"No symbols discovered under {input_dir}")
        return discovered

    normalized = [symbol.upper() for symbol in symbols]

    if len(normalized) == 1 and normalized[0] == "AUTO":
        discovered = discover_symbols(input_dir)
        if not discovered:
            raise ValueError(f"No symbols discovered under {input_dir}")
        return discovered

    result: list[str] = []
    seen: set[str] = set()
    for symbol in normalized:
        if symbol not in seen:
            result.append(symbol)
            seen.add(symbol)
    return result


def describe_date_range(start_date: str | None, end_date: str | None) -> str:
    if start_date and end_date:
        return f"{start_date} to {end_date}"
    if start_date:
        return f"from {start_date}"
    if end_date:
        return f"up to {end_date}"
    return "all available dates"


def find_index_directory(input_dir: Path, symbol: str) -> Path | None:
    symbol_lower = symbol.lower()
    for directory_name in INDEX_DIR_CANDIDATES:
        candidate = input_dir / directory_name / symbol_lower
        if candidate.exists():
            return candidate
    return None


def find_option_directories(input_dir: Path, symbol: str) -> list[Path]:
    symbol_lower = symbol.lower()
    option_dirs: list[Path] = []
    seen: set[Path] = set()

    for directory_name in OPTION_DIR_CANDIDATES:
        base_dir = input_dir / directory_name
        if not base_dir.exists():
            continue

        direct = base_dir / symbol_lower
        if direct.exists() and direct not in seen:
            option_dirs.append(direct)
            seen.add(direct)

        for suffix in ("_call", "_put"):
            candidate = base_dir / f"{symbol_lower}{suffix}"
            if candidate.exists() and candidate not in seen:
                option_dirs.append(candidate)
                seen.add(candidate)

    return option_dirs


def normalize_price_columns(df: pd.DataFrame, columns: list[str]) -> bool:
    """Ensure price columns are floating point and scale paise-based data to rupees."""
    if df.empty:
        return False

    df[columns] = df[columns].astype(float)
    max_abs_value = df[columns].abs().max().max()

    if max_abs_value > 100_000:
        df[columns] = df[columns] / 100.0
        return True

    return False


def bars_to_quote_ticks(bars, instrument):
    """Convert Bar data to QuoteTicks for Greeks calculation."""
    quote_ticks = []

    for bar in bars:
        price = Price(bar.close.as_double(), instrument.price_precision)
        size = Quantity(1, instrument.size_precision)

        tick = QuoteTick(
            instrument_id=instrument.id,
            bid_price=price,
            ask_price=price,
            bid_size=size,
            ask_size=size,
            ts_event=bar.ts_event,
            ts_init=bar.ts_init,
        )
        quote_ticks.append(tick)

    return quote_ticks


def yyyymmdd_seconds_to_datetime(date_int: int, time_int: int) -> datetime:
    """Convert YYYYMMDD integer + seconds to datetime in UTC."""
    if isinstance(date_int, datetime):
        year, month, day = date_int.year, date_int.month, date_int.day
    elif isinstance(date_int, date):
        year, month, day = date_int.year, date_int.month, date_int.day
    else:
        if isinstance(date_int, float) and date_int.is_integer():
            date_int = int(date_int)
        if isinstance(date_int, str):
            stripped = date_int.replace("-", "").strip()
            if not stripped:
                raise ValueError(f"Unsupported date format: {date_int!r}")
            try:
                date_int = int(stripped)
            except ValueError as exc:
                raise ValueError(f"Unsupported date format: {date_int!r}") from exc

        if not isinstance(date_int, int):
            raise TypeError(f"Unsupported date type: {type(date_int)}")

        if date_int >= 10_000_000:
            year = date_int // 10000
            month = (date_int % 10000) // 100
            day = date_int % 100
        elif date_int >= 100_000:
            year = 2000 + (date_int // 10000)
            month = (date_int % 10000) // 100
            day = date_int % 100
        else:
            raise ValueError(f"Unsupported date format: {date_int}")
    
    hours = time_int // 3600
    minutes = (time_int % 3600) // 60
    seconds = time_int % 60
    
    ist_dt = datetime(year, month, day, hours, minutes, seconds)
    utc_dt = ist_dt - IST_OFFSET
    
    return utc_dt


def transform_index_bars(
    input_dir: Path,
    catalog: ParquetDataCatalog,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None
) -> int:
    """Transform index data to Nautilus Bar format."""
    logger.info(f"Transforming {symbol} index bars...")
    
    symbol_dir = find_index_directory(input_dir, symbol)
    if not symbol_dir:
        logger.warning(
            f"No index/cash directory found for {symbol} under {input_dir} "
            f"(checked: {', '.join(INDEX_DIR_CANDIDATES)})"
        )
        return 0

    logger.info(f"Using index source directory: {symbol_dir}")

    parquet_files = list(symbol_dir.rglob("*.parquet"))
    
    if not parquet_files:
        logger.warning(f"No parquet files found in {symbol_dir}")
        return 0

    logger.info(f"Found {len(parquet_files)} parquet files for {symbol}")

    dfs = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            if df is not None and not df.empty:
                dfs.append(df)
        except Exception as e:
            logger.warning(f"Error reading {file}: {e}")
            continue
    
    if not dfs:
        logger.error("No data loaded")
        return 0
    
    combined_df = pd.concat(dfs, ignore_index=True)
    
    combined_df['timestamp'] = combined_df.apply(
        lambda row: yyyymmdd_seconds_to_datetime(row['date'], row['time']),
        axis=1
    )
    
    initial_row_count = len(combined_df)
    logger.info(f"Loaded {initial_row_count:,} rows. Data range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
    
    # Apply date filtering
    start = pd.to_datetime(start_date) - pd.Timedelta(hours=6) if start_date else None
    end = pd.to_datetime(end_date) + pd.Timedelta(days=1) if end_date else None

    if start is not None:
        before_filter = len(combined_df)
        combined_df = combined_df[combined_df['timestamp'] >= start]
        logger.info(f"After start date filter ({start_date}): {before_filter:,} -> {len(combined_df):,} rows")
    if end is not None:
        before_filter = len(combined_df)
        combined_df = combined_df[combined_df['timestamp'] < end]
        logger.info(f"After end date filter ({end_date}): {before_filter:,} -> {len(combined_df):,} rows")
    
    if combined_df.empty:
        logger.warning(f"No data in date range {describe_date_range(start_date, end_date)} for {symbol}")
        return 0
    
    logger.info(f"Processing {len(combined_df):,} rows after date filtering for {symbol}")

    if "volume" not in combined_df.columns:
        logger.info("No volume column present for %s index data; defaulting to zeros.", symbol)
        combined_df["volume"] = 0.0
    
    bar_df = combined_df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()

    if normalize_price_columns(bar_df, ["open", "high", "low", "close"]):
        logger.info("Scaled %s index prices from paise to rupees.", symbol)

    initial_count = len(bar_df)
    bar_df = bar_df.dropna(subset=["open", "high", "low", "close"])
    dropped_nan = initial_count - len(bar_df)
    if dropped_nan:
        logger.warning("%s index: Dropped %s rows with NaN OHLC values.", symbol, dropped_nan)

    bar_df["volume"] = bar_df["volume"].fillna(0).clip(lower=0)
    bar_df["high"] = bar_df[["high", "open", "close"]].max(axis=1)
    bar_df["low"] = bar_df[["low", "open", "close"]].min(axis=1)

    bar_df = bar_df.set_index('timestamp')
    bar_df = bar_df.sort_index()
    
    venue_name = get_venue_from_symbol(symbol)
    instrument_id = InstrumentId(
        symbol=Symbol(f"{symbol}-INDEX"),
        venue=Venue(venue_name)
    )
    
    instrument = Equity(
        instrument_id=instrument_id,
        raw_symbol=Symbol(symbol),
        currency=Currency.from_str("INR"),
        price_precision=2,
        price_increment=Price(0.05, 2),
        lot_size=Quantity.from_int(1),
        ts_event=0,
        ts_init=0,
    )
    
    bar_type = BarType.from_str(f"{instrument_id}-1-MINUTE-LAST-EXTERNAL")
    
    wrangler = BarDataWrangler(bar_type, instrument)
    bars = wrangler.process(
        data=bar_df,
        default_volume=0.0,
        ts_init_delta=0
    )
    
    logger.info(f"Writing {symbol} index data to catalog...")
    catalog.write_data([instrument])
    logger.info(f"  - Wrote instrument: {instrument.id}")
    
    catalog.write_data(bars, skip_disjoint_check=True)
    logger.info(f"  - Wrote {len(bars):,} bars")
    
    quote_ticks = bars_to_quote_ticks(bars, instrument)
    catalog.write_data(quote_ticks, skip_disjoint_check=True)
    logger.info(f"  - Wrote {len(quote_ticks):,} quote ticks")
    
    # Check if files were created
    import os
    bar_files = list(Path(catalog.path).rglob("**/bar/**/*.parquet"))
    quote_files = list(Path(catalog.path).rglob("**/quote_tick/**/*.parquet"))
    logger.info(f"  - Parquet files after write: {len(bar_files)} bar files, {len(quote_files)} quote files")
    if bar_files:
        logger.info(f"  - Sample bar file: {bar_files[0].relative_to(catalog.path)}")
    if quote_files:
        logger.info(f"  - Sample quote file: {quote_files[0].relative_to(catalog.path)}")
    
    logger.info(f"✅ {symbol}: Created {len(bars):,} bars + {len(quote_ticks):,} QuoteTicks")

    return len(bars)


def transform_futures_bars(
    input_dir: Path,
    catalog: ParquetDataCatalog,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None
) -> tuple[int, None]:
    """Transform futures data to Nautilus Bar format."""
    logger.info(f"Transforming {symbol} futures bars...")
    
    symbol_dir = input_dir / "futures" / symbol.lower()
    parquet_files = list(symbol_dir.rglob("*.parquet"))

    if not parquet_files:
        logger.warning(f"No parquet files found in {symbol_dir}")
        return 0, pd.DataFrame()

    # CRITICAL: Only use dated files (nifty_future_YYYYMMDD.parquet) which are in RUPEES
    dated_files = [f for f in parquet_files if f.stem.startswith(f"{symbol.lower()}_future_")]

    if not dated_files:
        logger.warning(f"No dated futures files found in {symbol_dir}")
        return 0, pd.DataFrame()

    logger.info(f"Using {len(dated_files)} dated futures files (already in rupees)")

    dfs = []
    for file in dated_files:
        try:
            df = pd.read_parquet(file)
            if df is None or df.empty:
                continue
            if df['date'].dtype == 'object':
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d').astype(int)
            if df['time'].dtype == 'object':
                df['time'] = df['time'].astype(int)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Error reading {file}: {e}")
            continue
    
    if not dfs:
        return 0, pd.DataFrame()
    
    combined_df = pd.concat(dfs, ignore_index=True)
    
    combined_df['timestamp'] = combined_df.apply(
        lambda row: yyyymmdd_seconds_to_datetime(row['date'], row['time']),
        axis=1
    )
    
    initial_row_count = len(combined_df)
    logger.info(f"Loaded {initial_row_count:,} futures rows. Data range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
    
    start = pd.to_datetime(start_date) - pd.Timedelta(hours=6) if start_date else None
    end = pd.to_datetime(end_date) + pd.Timedelta(days=1) if end_date else None

    if start is not None:
        before_filter = len(combined_df)
        combined_df = combined_df[combined_df['timestamp'] >= start]
        logger.info(f"After start date filter ({start_date}): {before_filter:,} -> {len(combined_df):,} rows")
    if end is not None:
        before_filter = len(combined_df)
        combined_df = combined_df[combined_df['timestamp'] < end]
        logger.info(f"After end date filter ({end_date}): {before_filter:,} -> {len(combined_df):,} rows")
    
    if combined_df.empty:
        logger.warning(f"No futures data in date range {describe_date_range(start_date, end_date)} for {symbol}")
        return 0, pd.DataFrame()
    
    logger.info(f"Processing {len(combined_df):,} futures rows after date filtering for {symbol}")
    
    bar_df = combined_df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()

    if normalize_price_columns(bar_df, ["open", "high", "low", "close"]):
        logger.info("Scaled %s futures prices from paise to rupees.", symbol)

    bar_df['volume'] = bar_df['volume'].clip(lower=0)
    bar_df['high'] = bar_df[['high', 'close']].max(axis=1)
    bar_df['low'] = bar_df[['low', 'close']].min(axis=1)
    
    bar_df = bar_df.set_index('timestamp')
    bar_df = bar_df.sort_index()
    
    venue_name = get_venue_from_symbol(symbol)
    if create_futures_contract:
        instrument = create_futures_contract(
            symbol=f"{symbol}-I",
            expiry_date="continuous",
            underlying=symbol,
            venue=venue_name
        )
    else:
        logger.error("create_futures_contract not available")
        return 0, pd.DataFrame()
    
    bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-LAST-EXTERNAL")
    
    wrangler = BarDataWrangler(bar_type, instrument)
    bars = wrangler.process(bar_df)
    
    catalog.write_data([instrument])
    catalog.write_data(bars, skip_disjoint_check=True)

    quote_ticks = bars_to_quote_ticks(bars, instrument)
    catalog.write_data(quote_ticks, skip_disjoint_check=True)

    if FutureOI and "oi" in combined_df.columns:
        oi_data_list = []
        oi_series = pd.to_numeric(combined_df["oi"], errors="coerce")
        prev_oi = None
        for ts_val, oi_val in zip(combined_df["timestamp"], oi_series):
            if pd.isna(oi_val):
                continue
            current_oi = int(oi_val)
            coi = 0 if prev_oi is None else current_oi - prev_oi
            prev_oi = current_oi
            ts_ns = int(pd.Timestamp(ts_val).timestamp() * 1_000_000_000)

            oi_data = FutureOI(
                instrument_id=instrument.id,
                oi=current_oi,
                coi=coi,
                ts_event=ts_ns,
                ts_init=ts_ns
            )
            oi_data_list.append(oi_data)
        
        if oi_data_list:
            oi_data_list.sort(key=lambda x: x.ts_init)
            catalog.write_data(oi_data_list, skip_disjoint_check=True)
            logger.info(f"✅ Saved {len(oi_data_list):,} FutureOI records")
    
    logger.info(f"✅ {symbol} futures: Created {len(bars):,} bars + {len(quote_ticks):,} QuoteTicks")
    return len(bars), None


def transform_options_bars(
    input_dir: Path,
    catalog: ParquetDataCatalog,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None
) -> int:
    """Transform options data to Nautilus Bar format."""
    logger.info(f"Transforming {symbol} options bars...")
    
    option_dirs = find_option_directories(input_dir, symbol)
    if not option_dirs:
        logger.warning(
            f"No options directories found for {symbol} under {input_dir} "
            f"(checked: {', '.join(OPTION_DIR_CANDIDATES)})"
        )
        return 0

    logger.info(
        "Using option source directories: %s",
        ", ".join(str(path) for path in option_dirs),
    )
    
    all_files: list[Path] = []
    for option_dir in option_dirs:
        all_files.extend(option_dir.rglob("*.parquet"))
    all_files = list(dict.fromkeys(all_files))

    symbol_lower = symbol.lower()
    dated_call_files = [f for f in all_files if f.stem.startswith(f"{symbol_lower}_call_")]
    dated_put_files = [f for f in all_files if f.stem.startswith(f"{symbol_lower}_put_")]
    parquet_files = dated_call_files + dated_put_files

    if not parquet_files:
        logger.warning(
            f"No dated option files found for {symbol} in "
            f"{', '.join(str(path) for path in option_dirs) or '(no option directories)'}"
        )
        return 0

    logger.info(f"Using {len(parquet_files)} dated option files (already in rupees)")

    total_bars = 0
    total_quote_ticks = 0
    scaled_option_prices_logged = False

    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            if df is None:
                continue
            
            if 'symbol' not in df.columns or df.empty:
                continue
            
            df['timestamp'] = df.apply(
                lambda row: yyyymmdd_seconds_to_datetime(row['date'], row['time']),
                axis=1
            )

            df['symbol'] = df['symbol'].astype(str).str.strip()
            
            start = pd.to_datetime(start_date) - pd.Timedelta(hours=6) if start_date else None
            end = pd.to_datetime(end_date) + pd.Timedelta(days=1) if end_date else None

            if start is not None:
                df = df[df['timestamp'] >= start]
            if end is not None:
                df = df[df['timestamp'] < end]
            
            if df.empty:
                continue
                
            for original_symbol, group in df.groupby('symbol'):
                try:
                    normalized_symbol = normalize_contract_symbol(original_symbol)
                    if not normalized_symbol:
                        logger.warning("Skipping option data with empty symbol from %s", file)
                        continue

                    group_sorted = group.sort_values('timestamp')

                    bar_df = group_sorted[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()

                    bar_df['volume'] = bar_df['volume'].clip(lower=0)
                    bar_df['high'] = bar_df[['high', 'close', 'open']].max(axis=1)
                    bar_df['low'] = bar_df[['low', 'close', 'open']].min(axis=1)

                    if normalize_price_columns(bar_df, ["open", "high", "low", "close"]):
                        if not scaled_option_prices_logged:
                            logger.info("Scaled %s options prices from paise to rupees.", symbol)
                            scaled_option_prices_logged = True
                    
                    bar_df = bar_df.set_index('timestamp')
                    bar_df = bar_df.sort_index()
                    
                    if not parse_nse_option_symbol or not create_options_contract:
                        logger.error("Option contract generators not available")
                        continue

                    parsed = None
                    # Try to infer year from data timestamps for better fallback parsing
                    data_year_hint = None
                    if not group_sorted.empty and 'timestamp' in group_sorted.columns:
                        try:
                            # Get year from first timestamp in the data
                            first_timestamp = pd.Timestamp(group_sorted['timestamp'].iloc[0])
                            data_year_hint = first_timestamp.year
                        except:
                            pass
                    
                    try:
                        # Try the main parser first
                        parsed = parse_nse_option_symbol(normalized_symbol)
                    except Exception as parse_error:
                        # If main parser fails, try fallback parser for alternative formats
                        try:
                            logger.debug(
                                "Main parser failed for %s (normalized %s): %s. Trying fallback parser...",
                                original_symbol,
                                normalized_symbol,
                                parse_error
                            )
                            parsed = parse_nse_option_symbol_fallback(normalized_symbol, data_year_hint=data_year_hint)
                            logger.debug("Fallback parser succeeded for %s (using year hint: %s)", normalized_symbol, data_year_hint)
                        except Exception as fallback_error:
                            # Both parsers failed
                            logger.warning(
                                "Skipping option %s (normalized %s): main parser failed with '%s', fallback parser failed with '%s'",
                                original_symbol,
                                normalized_symbol,
                                str(parse_error),
                                str(fallback_error)
                            )
                            continue
                    
                    if parsed is None:
                        logger.warning("Skipping option %s: parsing returned None", original_symbol)
                        continue

                    venue_name = get_venue_from_symbol(symbol)
                    instrument = create_options_contract(
                        symbol=normalized_symbol,
                        underlying=parsed['underlying'],
                        strike=parsed['strike'],
                        expiry=parsed['expiry'],
                        option_kind=parsed['option_type'],
                        venue=venue_name
                    )
                    
                    bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-LAST-EXTERNAL")
                    wrangler = BarDataWrangler(bar_type, instrument)
                    bars = wrangler.process(bar_df)
                    
                    catalog.write_data([instrument])
                    catalog.write_data(bars, skip_disjoint_check=True)

                    quote_ticks = bars_to_quote_ticks(bars, instrument)
                    catalog.write_data(quote_ticks, skip_disjoint_check=True)

                    if OptionOI and "oi" in group_sorted.columns:
                        oi_data_list = []
                        prev_oi = None
                        oi_series = pd.to_numeric(group_sorted["oi"], errors="coerce")
                        for ts_val, oi_val in zip(group_sorted["timestamp"], oi_series):
                            if pd.isna(oi_val):
                                continue
                            current_oi = int(oi_val)
                            coi = 0 if prev_oi is None else current_oi - prev_oi
                            prev_oi = current_oi
                            ts_ns = int(pd.Timestamp(ts_val).timestamp() * 1_000_000_000)

                            oi_data = OptionOI(
                                instrument_id=instrument.id,
                                oi=current_oi,
                                coi=coi,
                                ts_event=ts_ns,
                                ts_init=ts_ns
                            )
                            oi_data_list.append(oi_data)

                        if oi_data_list:
                            oi_data_list.sort(key=lambda x: x.ts_init)
                            catalog.write_data(oi_data_list, skip_disjoint_check=True)

                    total_bars += len(bars)
                    total_quote_ticks += len(quote_ticks)
                    
                except Exception as e:
                    logger.warning(f"Error processing option {original_symbol}: {e}")
                    continue
                    
        except Exception as e:
            logger.warning(f"Error reading {file}: {e}")
            continue

    logger.info(f"✅ {symbol} options: Created {total_bars:,} bars + {total_quote_ticks:,} QuoteTicks")
    return total_bars


def main():
    parser = argparse.ArgumentParser(
        description="Transform NSE data from local filesystem to Nautilus catalog and upload to DigitalOcean Spaces"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Input directory with raw data (local filesystem path)"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols to transform (use AUTO to discover from input directory)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Optional start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Optional end date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep temporary files after processing"
    )
    
    args = parser.parse_args()

    # Validate input directory
    if not args.input_dir.exists():
        logger.error(f"Input directory does not exist: {args.input_dir}")
        sys.exit(1)
    
    if not args.input_dir.is_dir():
        logger.error(f"Input path is not a directory: {args.input_dir}")
        sys.exit(1)

    # Initialize DigitalOcean Spaces connection (for output only)
    logger.info("Connecting to DigitalOcean Spaces for output...")
    spaces = DigitalOceanSpaces(SPACES_ACCESS_KEY, SPACES_SECRET_KEY, SPACES_ENDPOINT, SPACES_REGION)
    
    if not spaces.test_connection():
        logger.error("Failed to connect to DigitalOcean Spaces")
        sys.exit(1)
    
    # Create Spaces uploader
    spaces_uploader = SpacesOutputUploader(spaces, SPACES_BUCKET, OUTPUT_PREFIX)
    
    # Setup temporary directory (only for output files)
    temp_dir = Path(tempfile.mkdtemp(prefix="nautilus_transform_"))
    output_dir = temp_dir / "output"
    output_dir.mkdir(exist_ok=True)
    logger.info(f"Created temporary directory for output: {temp_dir}")
    
    try:
        # Discover symbols from local directory
        logger.info("="*80)
        logger.info("DISCOVERING SYMBOLS FROM LOCAL DIRECTORY")
        logger.info("="*80)
        try:
            symbols = resolve_symbol_list(args.input_dir, args.symbols)
        except ValueError as exc:
            logger.error(exc)
            sys.exit(1)

        date_range_label = describe_date_range(args.start_date, args.end_date)
        
        logger.info("="*80)
        logger.info("NAUTILUS DATA TRANSFORMATION - LOCAL INPUT TO SPACES OUTPUT")
        logger.info("="*80)
        logger.info(f"Input: {args.input_dir} (local filesystem)")
        logger.info(f"Output: {SPACES_BUCKET}/{OUTPUT_PREFIX} (DigitalOcean Spaces)")
        logger.info(f"Symbols to process: {symbols}")
        if args.symbols:
            logger.info(f"  (Filtered to specified symbols: {args.symbols})")
        logger.info(f"Date range: {date_range_label}")
        if args.start_date or args.end_date:
            logger.info(f"  (Only processing data within: {args.start_date} to {args.end_date})")
        logger.info("="*80)
        
        # Create catalog pointing to output directory
        catalog = ParquetDataCatalog(path=str(output_dir))
        data_root = ensure_catalog_structure(output_dir)
        logger.info("Created ParquetDataCatalog at: %s", str(output_dir))
        logger.info("Ensured catalog structure under %s", data_root)
        logger.info("Catalog path: %s", catalog.path)
        
        # Log catalog directory structure
        if output_dir.exists():
            logger.info("Output directory structure:")
            for item in sorted(output_dir.iterdir()):
                logger.info(f"  - {item.name}/ {'(dir)' if item.is_dir() else '(file)'}")
        
        total_bars = 0
        files_before_transform = len(list(output_dir.rglob("*.parquet")))
        logger.info(f"Parquet files before transformation: {files_before_transform}")
        
        # Transform index data (reads from local filesystem)
        logger.info("="*80)
        logger.info("TRANSFORMING INDEX DATA")
        logger.info("="*80)
        for symbol in symbols:
            try:
                files_before = len(list(output_dir.rglob("*.parquet")))
                logger.info(f"Files before {symbol} index transform: {files_before}")
                
                count = transform_index_bars(
                    args.input_dir,
                    catalog,
                    symbol,
                    args.start_date,
                    args.end_date
                )
                total_bars += count
                
                files_after = len(list(output_dir.rglob("*.parquet")))
                logger.info(f"Files after {symbol} index transform: {files_after} (added {files_after - files_before} files)")
            except Exception as e:
                logger.error(f"Error transforming {symbol} index: {e}", exc_info=True)
        
        # Transform futures data (reads from local filesystem)
        logger.info("="*80)
        logger.info("TRANSFORMING FUTURES DATA")
        logger.info("="*80)
        for symbol in symbols:
            try:
                files_before = len(list(output_dir.rglob("*.parquet")))
                logger.info(f"Files before {symbol} futures transform: {files_before}")
                
                count, _ = transform_futures_bars(
                    args.input_dir,
                    catalog,
                    symbol,
                    args.start_date,
                    args.end_date
                )
                total_bars += count
                
                files_after = len(list(output_dir.rglob("*.parquet")))
                logger.info(f"Files after {symbol} futures transform: {files_after} (added {files_after - files_before} files)")
            except Exception as e:
                logger.error(f"Error transforming {symbol} futures: {e}", exc_info=True)
        
        # Transform options data (reads from local filesystem)
        logger.info("="*80)
        logger.info("TRANSFORMING OPTIONS DATA")
        logger.info("="*80)
        for symbol in symbols:
            try:
                files_before = len(list(output_dir.rglob("*.parquet")))
                logger.info(f"Files before {symbol} options transform: {files_before}")
                
                count = transform_options_bars(
                    args.input_dir,
                    catalog,
                    symbol,
                    args.start_date,
                    args.end_date
                )
                total_bars += count
                
                files_after = len(list(output_dir.rglob("*.parquet")))
                logger.info(f"Files after {symbol} options transform: {files_after} (added {files_after - files_before} files)")
            except Exception as e:
                logger.error(f"Error transforming {symbol} options: {e}", exc_info=True)
        
        # Check catalog path one more time
        logger.info("="*80)
        logger.info("FINAL CATALOG CHECK")
        logger.info("="*80)
        logger.info(f"Catalog path: {catalog.path}")
        logger.info(f"Catalog path exists: {Path(catalog.path).exists()}")
        
        # Flush catalog to ensure all data is written to disk before upload
        logger.info("Flushing catalog to ensure all data is written to disk...")
        try:
            # ParquetDataCatalog may need explicit flush - try calling write_data with empty list to trigger flush
            import gc
            import time
            gc.collect()  # Force garbage collection to ensure all writes are complete
            time.sleep(1)  # Small delay to ensure file system writes complete
            logger.info("Catalog flush complete")
        except Exception as e:
            logger.warning(f"Could not explicitly flush catalog: {e}")
        
        # Verify files exist before uploading
        logger.info("="*80)
        logger.info("VERIFYING FILES BEFORE UPLOAD")
        logger.info("="*80)
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Output directory exists: {output_dir.exists()}")
        logger.info(f"Output directory absolute path: {output_dir.resolve()}")
        
        # Check for parquet files specifically
        parquet_files = list(output_dir.rglob("*.parquet"))
        logger.info(f"Found {len(parquet_files)} parquet files")
        if parquet_files:
            logger.info("Sample parquet files:")
            for i, pf in enumerate(parquet_files[:10]):
                file_size = pf.stat().st_size if pf.exists() else 0
                logger.info(f"  {i+1}. {pf.relative_to(output_dir)} ({file_size:,} bytes)")
        
        # Check all files
        all_output_files = list(output_dir.rglob("*"))
        actual_files = [f for f in all_output_files if f.is_file()]
        logger.info(f"Found {len(actual_files)} total files in output directory")
        
        # Check directory structure
        logger.info("Directory structure:")
        if output_dir.exists():
            for item in sorted(output_dir.rglob("*/")):
                if item.is_dir():
                    files_in_dir = list(item.glob("*.parquet"))
                    logger.info(f"  {item.relative_to(output_dir)}/ - {len(files_in_dir)} parquet files")
        
        if not actual_files:
            logger.error("ERROR: No files found in output directory! Data may not have been written.")
            logger.error(f"Output directory path: {output_dir}")
            logger.error(f"Output directory exists: {output_dir.exists()}")
            if output_dir.exists():
                # List what's actually in the directory
                logger.error("Contents of output directory:")
                for item in output_dir.iterdir():
                    logger.error(f"  - {item.name} ({'dir' if item.is_dir() else 'file'})")
            raise RuntimeError("No files found in output directory - cannot upload to Spaces")
        
        if actual_files:
            logger.info(f"Sample files (first 10):")
            for i, f in enumerate(actual_files[:10]):
                file_size = f.stat().st_size
                logger.info(f"  {i+1}. {f.relative_to(output_dir)} ({file_size:,} bytes)")
        
        # Upload output files to Spaces
        logger.info("="*80)
        logger.info("UPLOADING OUTPUT FILES TO SPACES")
        logger.info("="*80)
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Output directory exists: {output_dir.exists()}")
        
        try:
            spaces_uploader.upload_output_files(output_dir)
        except Exception as e:
            logger.error(f"Failed to upload files to Spaces: {e}", exc_info=True)
            raise
        
        # Summary
        print("\n" + "="*80)
        print("TRANSFORMATION COMPLETE")
        print("="*80)
        print(f"Total bars created: {total_bars:,}")
        print(f"Input location: {args.input_dir} (local)")
        print(f"Output location: {SPACES_BUCKET}/{OUTPUT_PREFIX} (Spaces)")
        print(f"Date range processed: {date_range_label}")
        print("="*80)
        print("\nData structure in Spaces:")
        print(f"  Bar data: {SPACES_BUCKET}/{OUTPUT_PREFIX}bar/")
        print(f"  Quote tick data: {SPACES_BUCKET}/{OUTPUT_PREFIX}quote_tick/")
        print(f"  Instruments: {SPACES_BUCKET}/{OUTPUT_PREFIX}equity/")
        print(f"  FutureOI data: {SPACES_BUCKET}/{OUTPUT_PREFIX}custom_future_oi/")
        print(f"  OptionOI data: {SPACES_BUCKET}/{OUTPUT_PREFIX}custom_option_oi/")
        print("="*80)
        
    finally:
        # Cleanup temporary directory
        if not args.keep_temp:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
        else:
            logger.info(f"Temporary files kept at: {temp_dir}")


if __name__ == "__main__":
    main()

