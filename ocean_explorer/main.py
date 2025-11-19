#!/usr/bin/env python3
"""
Official Nautilus Data Transformation Pattern with DigitalOcean Spaces Integration

Reads input from DigitalOcean Spaces and writes output back to Spaces.
Input: historical-db-1min/raw/parquet_data/
Output: historical-db-1min/raw/data/
"""

import sys
import tempfile
import shutil
from pathlib import Path
from datetime import date, datetime, timedelta
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

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

# Import DigitalOcean Spaces connection
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

# DigitalOcean Spaces configuration
SPACES_ACCESS_KEY = "DO00CDX8Z7BFTQJ9W2AZ"
SPACES_SECRET_KEY = "kR159s1x7Xjfd6RhVIyw8X34UGLIFKSzP7/0fG6Yt9I"
SPACES_ENDPOINT = "https://blr1.digitaloceanspaces.com"
SPACES_REGION = "blr1"
SPACES_BUCKET = "historical-db-1min"
INPUT_PREFIX = "raw/parquet_data/"
OUTPUT_PREFIX = "raw/data/"

# IST offset (5 hours 30 minutes)
IST_OFFSET = timedelta(hours=5, minutes=30)

INDEX_DIR_CANDIDATES = ("index", "cash")
OPTION_DIR_CANDIDATES = ("option", "options")
CATALOG_DATA_SUBDIRS = (
    "bar",
    "quote_tick",
    "equity",
    "futures_contract",
    "option_contract",
    "custom_future_oi",
    "custom_option_oi",
)
OPTION_SYMBOL_SUFFIXES = (".NSE", ".BSE", ".NFO", ".FO", ".MCX", ".CDS")


class SpacesFileSystem:
    """Wrapper to make DigitalOcean Spaces work like a filesystem for the transformation code"""
    
    def __init__(self, spaces: DigitalOceanSpaces, bucket: str, input_prefix: str, output_prefix: str):
        self.spaces = spaces
        self.bucket = bucket
        self.input_prefix = input_prefix.rstrip('/') + '/' if input_prefix else ''
        self.output_prefix = output_prefix.rstrip('/') + '/' if output_prefix else ''
        self.temp_dir = None
        
    def setup_temp_dir(self) -> Path:
        """Create temporary directory for processing (only for output files)"""
        if self.temp_dir is None:
            self.temp_dir = Path(tempfile.mkdtemp(prefix="nautilus_transform_"))
            logger.info(f"Created temporary directory for output: {self.temp_dir}")
        return self.temp_dir
    
    def cleanup_temp_dir(self):
        """Clean up temporary directory"""
        if self.temp_dir and self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
            self.temp_dir = None
    
    def read_parquet(self, object_key: str):
        """Read parquet file directly from Spaces"""
        return self.spaces.read_parquet_from_spaces(self.bucket, object_key)
    
    def get_parquet_files_for_symbol(self, symbol: str, data_type: str, start_date: str = None, end_date: str = None) -> list[str]:
        """
        Get list of parquet file keys for a symbol and data type, optionally filtered by date
        
        Args:
            symbol: Symbol name (e.g., "NIFTY")
            data_type: "cash", "futures", or "options"
            start_date: Optional start date (YYYY-MM-DD) to filter filenames
            end_date: Optional end date (YYYY-MM-DD) to filter filenames
            
        Returns:
            List of object keys (full paths in Spaces)
        """
        symbol_lower = symbol.lower()
        files = []
        
        # Parse dates for filename filtering
        start_date_obj = None
        end_date_obj = None
        if start_date:
            try:
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            except:
                pass
        if end_date:
            try:
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            except:
                pass
        
        def extract_date_from_filename(filename: str) -> datetime | None:
            """Extract date from filename patterns like: nifty_20240101.parquet, nifty_future_20240101.parquet"""
            # Try to find YYYYMMDD pattern
            match = re.search(r'(\d{8})', filename)
            if match:
                try:
                    return datetime.strptime(match.group(1), '%Y%m%d')
                except:
                    pass
            return None
        
        def should_include_file(key: str) -> bool:
            """Check if file should be included based on date filtering"""
            if not start_date_obj and not end_date_obj:
                return True
            
            file_date = extract_date_from_filename(key)
            if file_date is None:
                # If we can't extract date, include it (will be filtered later)
                return True
            
            if start_date_obj and file_date < start_date_obj:
                return False
            if end_date_obj and file_date > end_date_obj:
                return False
            return True
        
        if data_type == "cash":
            for dir_name in INDEX_DIR_CANDIDATES:
                prefix = f"{self.input_prefix}{dir_name}/{symbol_lower}/"
                objects = self.spaces.list_objects(self.bucket, prefix=prefix, max_keys=10000, show_details=False, silent=True)
                for obj in objects:
                    if isinstance(obj, dict):
                        key = obj.get('Key', '')
                        if key and key.endswith('.parquet') and should_include_file(key):
                            files.append(key)
        elif data_type == "futures":
            prefix = f"{self.input_prefix}futures/{symbol_lower}/"
            objects = self.spaces.list_objects(self.bucket, prefix=prefix, max_keys=10000, show_details=False, silent=True)
            for obj in objects:
                if isinstance(obj, dict):
                    key = obj.get('Key', '')
                    if key and key.endswith('.parquet') and f"{symbol_lower}_future_" in key and should_include_file(key):
                        files.append(key)
        elif data_type == "options":
            for dir_name in OPTION_DIR_CANDIDATES:
                prefix = f"{self.input_prefix}{dir_name}/{symbol_lower}"
                objects = self.spaces.list_objects(self.bucket, prefix=prefix, max_keys=10000, show_details=False, silent=True)
                for obj in objects:
                    if isinstance(obj, dict):
                        key = obj.get('Key', '')
                        if key and key.endswith('.parquet') and (f"{symbol_lower}_call_" in key or f"{symbol_lower}_put_" in key) and should_include_file(key):
                            files.append(key)
        
        return files
    
    def read_parquet_files_parallel(self, object_keys: list[str], max_workers: int = 10) -> list:
        """
        Read multiple parquet files from Spaces in parallel
        
        Args:
            object_keys: List of object keys to read
            max_workers: Maximum number of parallel workers
            
        Returns:
            List of DataFrames (None for failed reads)
        """
        def read_single_file(object_key: str):
            try:
                return self.read_parquet(object_key)
            except Exception as e:
                logger.warning(f"Error reading {object_key}: {e}")
                return None
        
        dfs = []
        total = len(object_keys)
        
        if total == 0:
            return []
        
        logger.info(f"Reading {total} files in parallel (max {max_workers} workers)...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_key = {executor.submit(read_single_file, key): key for key in object_keys}
            
            # Process completed tasks with progress
            completed = 0
            for future in as_completed(future_to_key):
                completed += 1
                df = future.result()
                if df is not None and not df.empty:
                    dfs.append(df)
                
                # Log progress every 50 files or at completion
                if completed % 50 == 0 or completed == total:
                    logger.info(f"Progress: {completed}/{total} files read ({len(dfs)} successful)")
        
        logger.info(f"Successfully read {len(dfs)}/{total} files")
        return dfs
    
    # Removed download_input_files - we'll read directly from Spaces now
    
    def upload_output_files(self, local_dir: Path):
        """Upload output files from local directory to Spaces"""
        logger.info(f"Uploading files to Spaces: {self.bucket}/{self.output_prefix}")
        
        # Find all files in local directory
        uploaded_count = 0
        for local_file in local_dir.rglob("*"):
            if local_file.is_file():
                # Get relative path from local_dir
                relative_path = local_file.relative_to(local_dir)
                # Construct Spaces key
                spaces_key = self.output_prefix + str(relative_path).replace('\\', '/')
                
                try:
                    self.spaces.upload_file(str(local_file), self.bucket, spaces_key)
                    uploaded_count += 1
                except Exception as e:
                    logger.warning(f"Failed to upload {local_file} to {spaces_key}: {e}")
        
        logger.info(f"Uploaded {uploaded_count} files to Spaces")
    
    def list_objects_in_prefix(self, prefix: str) -> list:
        """List objects in a specific prefix"""
        full_prefix = self.input_prefix + prefix
        objects = self.spaces.list_objects(self.bucket, prefix=full_prefix, max_keys=10000, show_details=False, silent=True)
        # list_objects returns a list of objects, but we need the raw object data
        # If it's already a list of dicts with 'Key', return as is
        if objects and isinstance(objects[0], dict) and 'Key' in objects[0]:
            return objects
        # Otherwise, convert if needed
        return objects if isinstance(objects, list) else []


def ensure_catalog_structure(root: Path) -> Path:
    """Ensure Nautilus catalog subdirectories exist (creates them if missing)."""
    data_root = root / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    for subdir in CATALOG_DATA_SUBDIRS:
        (data_root / subdir).mkdir(parents=True, exist_ok=True)
    return data_root


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


def _extract_symbol_from_name(name: str) -> str | None:
    base = name.split("_", 1)[0].strip()
    if not base:
        return None

    cleaned = "".join(ch for ch in base if ch.isalnum())
    if not cleaned or not cleaned[0].isalpha() or not any(ch.isalpha() for ch in cleaned):
        return None

    return cleaned.upper()


def discover_symbols_from_spaces(spaces_fs: SpacesFileSystem) -> list[str]:
    """Discover available symbols by scanning Spaces objects."""
    symbols: set[str] = set()

    def add_symbol(name: str) -> None:
        candidate = _extract_symbol_from_name(name)
        if candidate:
            symbols.add(candidate)

    # List objects in cash/index directories
    for directory_name in INDEX_DIR_CANDIDATES:
        objects = spaces_fs.list_objects_in_prefix(f"{directory_name}/")
        for obj in objects:
            # Handle both dict format and extract key
            if isinstance(obj, dict):
                key = obj.get('Key', '')
            else:
                key = str(obj)
            
            if not key:
                continue
                
            # Extract symbol from path like "raw/parquet_data/cash/nifty/file.parquet"
            parts = key.split('/')
            if len(parts) >= 3:
                symbol_candidate = parts[-2]  # Directory name
                add_symbol(symbol_candidate)

    # List objects in futures directory
    objects = spaces_fs.list_objects_in_prefix("futures/")
    for obj in objects:
        if isinstance(obj, dict):
            key = obj.get('Key', '')
        else:
            key = str(obj)
            
        if not key:
            continue
            
        parts = key.split('/')
        if len(parts) >= 3:
            symbol_candidate = parts[-2]
            add_symbol(symbol_candidate)

    # List objects in options directories
    for directory_name in OPTION_DIR_CANDIDATES:
        objects = spaces_fs.list_objects_in_prefix(f"{directory_name}/")
        for obj in objects:
            if isinstance(obj, dict):
                key = obj.get('Key', '')
            else:
                key = str(obj)
                
            if not key:
                continue
                
            parts = key.split('/')
            if len(parts) >= 3:
                symbol_candidate = parts[-2]
                add_symbol(symbol_candidate)

    if not symbols:
        logger.warning("No symbols discovered from Spaces")
    else:
        logger.info(f"Discovered {len(symbols)} symbols from Spaces: {sorted(symbols)}")

    return sorted(symbols)


def discover_symbols(input_dir: Path) -> list[str]:
    """Discover available symbols by scanning local directory (for compatibility)."""
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

    return sorted(symbols)


def resolve_symbol_list(input_dir: Path | None, symbols: list[str] | None, spaces_fs: SpacesFileSystem = None) -> list[str]:
    """Resolve user-provided symbols or auto-discover them."""
    if not symbols:
        if spaces_fs:
            discovered = discover_symbols_from_spaces(spaces_fs)
        elif input_dir:
            discovered = discover_symbols(input_dir)
        else:
            raise ValueError("Either spaces_fs or input_dir must be provided for symbol discovery")
        if not discovered:
            raise ValueError(f"No symbols discovered")
        return discovered

    normalized = [symbol.upper() for symbol in symbols]

    if len(normalized) == 1 and normalized[0] == "AUTO":
        if spaces_fs:
            discovered = discover_symbols_from_spaces(spaces_fs)
        else:
            discovered = discover_symbols(input_dir)
        if not discovered:
            raise ValueError(f"No symbols discovered")
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


# Keep all the transform functions from the original main.py
# They work with local Path objects, which we'll provide from downloaded files

def transform_index_bars(
    spaces_fs: SpacesFileSystem,
    catalog: ParquetDataCatalog,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
    max_workers: int = 10
) -> int:
    """Transform index data to Nautilus Bar format - reads directly from Spaces with parallel processing."""
    logger.info(f"Transforming {symbol} index bars...")
    
    # Get parquet files from Spaces (with date filtering in filename)
    parquet_keys = spaces_fs.get_parquet_files_for_symbol(symbol, "cash", start_date, end_date)
    
    if not parquet_keys:
        logger.warning(
            f"No index/cash parquet files found for {symbol} in Spaces "
            f"(checked: {', '.join(INDEX_DIR_CANDIDATES)})"
        )
        return 0

    logger.info(f"Found {len(parquet_keys)} parquet files for {symbol} in Spaces (after date filtering)")

    # Read files in parallel
    dfs = spaces_fs.read_parquet_files_parallel(parquet_keys, max_workers=max_workers)
    
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
    
    instrument_id = InstrumentId(
        symbol=Symbol(f"{symbol}-INDEX"),
        venue=Venue("NSE")
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
    
    catalog.write_data([instrument])
    catalog.write_data(bars, skip_disjoint_check=True)

    quote_ticks = bars_to_quote_ticks(bars, instrument)
    catalog.write_data(quote_ticks, skip_disjoint_check=True)
    logger.info(f"✅ {symbol}: Created {len(bars):,} bars + {len(quote_ticks):,} QuoteTicks")

    return len(bars)


def transform_futures_bars(
    spaces_fs: SpacesFileSystem,
    catalog: ParquetDataCatalog,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
    output_dir: Path = None,
    max_workers: int = 10
) -> tuple[int, None]:
    """Transform futures data to Nautilus Bar format - reads directly from Spaces with parallel processing."""
    logger.info(f"Transforming {symbol} futures bars...")
    
    # Get parquet files from Spaces (with date filtering in filename)
    parquet_keys = spaces_fs.get_parquet_files_for_symbol(symbol, "futures", start_date, end_date)

    if not parquet_keys:
        logger.warning(f"No dated futures files found for {symbol} in Spaces")
        return 0, pd.DataFrame()

    logger.info(f"Using {len(parquet_keys)} dated futures files from Spaces (after date filtering, already in rupees)")

    # Read files in parallel
    dfs_raw = spaces_fs.read_parquet_files_parallel(parquet_keys, max_workers=max_workers)
    
    # Process date/time columns
    dfs = []
    for df in dfs_raw:
        if df is None or df.empty:
            continue
        if df['date'].dtype == 'object':
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d').astype(int)
        if df['time'].dtype == 'object':
            df['time'] = df['time'].astype(int)
        dfs.append(df)
    
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
    
    if create_futures_contract:
        instrument = create_futures_contract(
            symbol=f"{symbol}-I",
            expiry_date="continuous",
            underlying=symbol,
            venue="NSE"
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
    spaces_fs: SpacesFileSystem,
    catalog: ParquetDataCatalog,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
    max_workers: int = 10
) -> int:
    """Transform options data to Nautilus Bar format - reads directly from Spaces with parallel processing."""
    logger.info(f"Transforming {symbol} options bars...")
    
    # Get parquet files from Spaces (with date filtering in filename)
    parquet_keys = spaces_fs.get_parquet_files_for_symbol(symbol, "options", start_date, end_date)

    if not parquet_keys:
        logger.warning(
            f"No dated option files found for {symbol} in Spaces "
            f"(checked: {', '.join(OPTION_DIR_CANDIDATES)})"
        )
        return 0

    logger.info(f"Using {len(parquet_keys)} dated option files from Spaces (after date filtering, already in rupees)")

    # Read files in parallel
    dfs_raw = spaces_fs.read_parquet_files_parallel(parquet_keys, max_workers=max_workers)

    total_bars = 0
    total_quote_ticks = 0
    scaled_option_prices_logged = False

    for df in dfs_raw:
        if df is None:
            continue
        
        if 'symbol' not in df.columns or df.empty:
            continue
        
        try:
            df['timestamp'] = df.apply(
                lambda row: yyyymmdd_seconds_to_datetime(row['date'], row['time']),
                axis=1
            )

            df['symbol'] = df['symbol'].astype(str).str.strip()
            
            initial_rows = len(df)
            
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

                    try:
                        # parse_nse_option_symbol only takes the symbol string
                        parsed = parse_nse_option_symbol(normalized_symbol)
                    except Exception as parse_error:
                        logger.error(
                            "Skipping option %s (normalized %s): %s",
                            original_symbol,
                            normalized_symbol,
                            parse_error
                        )
                        continue

                    instrument = create_options_contract(
                        symbol=normalized_symbol,
                        underlying=parsed['underlying'],
                        strike=parsed['strike'],
                        expiry=parsed['expiry'],
                        option_kind=parsed['option_type'],
                        venue="NSE"
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
            logger.warning(f"Error processing file: {e}")
            continue

    logger.info(f"✅ {symbol} options: Created {total_bars:,} bars + {total_quote_ticks:,} QuoteTicks")
    return total_bars


def main():
    parser = argparse.ArgumentParser(
        description="Transform NSE data to Nautilus catalog using DigitalOcean Spaces"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols to transform (use AUTO to discover from Spaces)"
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
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of parallel workers for reading files (default: 10)"
    )
    
    args = parser.parse_args()

    # Initialize DigitalOcean Spaces connection
    logger.info("Connecting to DigitalOcean Spaces...")
    spaces = DigitalOceanSpaces(SPACES_ACCESS_KEY, SPACES_SECRET_KEY, SPACES_ENDPOINT, SPACES_REGION)
    
    if not spaces.test_connection():
        logger.error("Failed to connect to DigitalOcean Spaces")
        sys.exit(1)
    
    # Create Spaces filesystem wrapper
    spaces_fs = SpacesFileSystem(spaces, SPACES_BUCKET, INPUT_PREFIX, OUTPUT_PREFIX)
    
    # Setup temporary directory (only for output files)
    temp_dir = spaces_fs.setup_temp_dir()
    output_dir = temp_dir / "output"
    output_dir.mkdir(exist_ok=True)
    
    try:
        # Discover symbols directly from Spaces (no download needed)
        logger.info("="*80)
        logger.info("DISCOVERING SYMBOLS FROM SPACES")
        logger.info("="*80)
        try:
            symbols = resolve_symbol_list(None, args.symbols, spaces_fs)
        except ValueError as exc:
            logger.error(exc)
            sys.exit(1)

        date_range_label = describe_date_range(args.start_date, args.end_date)
        
        logger.info("="*80)
        logger.info("NAUTILUS DATA TRANSFORMATION - DIGITALOCEAN SPACES")
        logger.info("="*80)
        logger.info(f"Input: {SPACES_BUCKET}/{INPUT_PREFIX}")
        logger.info(f"Output: {SPACES_BUCKET}/{OUTPUT_PREFIX}")
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
        logger.info("Ensured catalog structure under %s", data_root)
        
        total_bars = 0
        
        # Transform index data (reads directly from Spaces with parallel processing)
        for symbol in symbols:
            try:
                count = transform_index_bars(
                    spaces_fs,
                    catalog,
                    symbol,
                    args.start_date,
                    args.end_date,
                    max_workers=args.max_workers
                )
                total_bars += count
            except Exception as e:
                logger.error(f"Error transforming {symbol} index: {e}", exc_info=True)
        
        # Transform futures data (reads directly from Spaces with parallel processing)
        for symbol in symbols:
            try:
                count, _ = transform_futures_bars(
                    spaces_fs,
                    catalog,
                    symbol,
                    args.start_date,
                    args.end_date,
                    max_workers=args.max_workers
                )
                total_bars += count
            except Exception as e:
                logger.error(f"Error transforming {symbol} futures: {e}", exc_info=True)
        
        # Transform options data (reads directly from Spaces with parallel processing)
        for symbol in symbols:
            try:
                count = transform_options_bars(
                    spaces_fs,
                    catalog,
                    symbol,
                    args.start_date,
                    args.end_date,
                    max_workers=args.max_workers
                )
                total_bars += count
            except Exception as e:
                logger.error(f"Error transforming {symbol} options: {e}", exc_info=True)
        
        # Upload output files to Spaces
        logger.info("="*80)
        logger.info("UPLOADING OUTPUT FILES TO SPACES")
        logger.info("="*80)
        spaces_fs.upload_output_files(output_dir)
        
        # Summary
        print("\n" + "="*80)
        print("TRANSFORMATION COMPLETE")
        print("="*80)
        print(f"Total bars created: {total_bars:,}")
        print(f"Input location: {SPACES_BUCKET}/{INPUT_PREFIX}")
        print(f"Output location: {SPACES_BUCKET}/{OUTPUT_PREFIX}")
        print(f"Date range processed: {date_range_label}")
        print("="*80)
        print("\nData structure in Spaces:")
        print(f"  Bar data: {SPACES_BUCKET}/{OUTPUT_PREFIX}data/bar/")
        print(f"  Quote tick data: {SPACES_BUCKET}/{OUTPUT_PREFIX}data/quote_tick/")
        print(f"  Instruments: {SPACES_BUCKET}/{OUTPUT_PREFIX}data/equity/")
        print(f"  FutureOI data: {SPACES_BUCKET}/{OUTPUT_PREFIX}data/custom_future_oi/")
        print(f"  OptionOI data: {SPACES_BUCKET}/{OUTPUT_PREFIX}data/custom_option_oi/")
        print("="*80)
        
    finally:
        # Cleanup temporary directory
        if not args.keep_temp:
            spaces_fs.cleanup_temp_dir()
        else:
            logger.info(f"Temporary files kept at: {temp_dir}")


if __name__ == "__main__":
    main()

