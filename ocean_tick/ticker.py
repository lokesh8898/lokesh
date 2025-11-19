#!/usr/bin/env python3
"""
Official Nautilus Data Transformation Pattern

Follows the verified pattern from nautilus_trader/examples/backtest/
- example_01_load_bars_from_custom_csv/run_example.py
- example_04_using_data_catalog/run_example.py

Transforms locally stored NSE futures and options parquet files into the
Nautilus catalog format (skips cash/index data) and can optionally upload
the resulting catalog to DigitalOcean Spaces.

Expected local layout:
  - futures/[symbol]/YYYY/MM/*.parquet
  - options/[symbol_call|symbol_put]/YYYY/MM/*.parquet
"""

import os
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
import argparse
import logging
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
import shlex
import shutil
import subprocess

# Add repository root to path so package imports resolve when run as a script
# Try to find project root, fallback to current directory if not deep enough
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except IndexError:
    PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config as BotoConfig
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.data import BarType, QuoteTick
from nautilus_trader.model.instruments import Equity, OptionContract, FuturesContract
from nautilus_trader.model.objects import Price, Quantity, Currency
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import BarDataWrangler

# Import our contract generators
from marvelquant_data.utils.contract_generators import (
    create_options_contract,
    create_futures_contract,
    parse_nse_option_symbol
)

# Import custom data types
from marvelquant_data.data_types import OptionOI, FutureOI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

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

VENUE_OVERRIDES = {
    "SENSEX": "BSE",
    "CRUDEOIL": "MCX",
    "NATURALGAS": "MCX",
    "GOLDM": "MCX",
    "SILVERM": "MCX",
}

# BarType metadata suffix to ensure valid BarSpecification parsing.
# Produces strings like "NIFTY.NSE-1-TICK-LAST-EXTERNAL".
BAR_TYPE_SUFFIX = "-1-TICK-LAST-EXTERNAL"

def ensure_catalog_structure(root: Path) -> Path:
    """
    Ensure Nautilus catalog subdirectories exist (creates them if missing).
    """
    data_root = root
    data_root.mkdir(parents=True, exist_ok=True)
    for subdir in CATALOG_DATA_SUBDIRS:
        (data_root / subdir).mkdir(parents=True, exist_ok=True)
    return data_root


def _merge_paths(source: Path, destination: Path) -> None:
    """
    Move `source` into `destination`, merging contents when both are directories.
    """
    if not destination.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))
        return

    if source.is_file():
        if destination.is_dir():
            raise IsADirectoryError(
                f"Cannot overwrite directory '{destination}' with file '{source}'."
            )
        destination.unlink()
        shutil.move(str(source), str(destination))
        return

    if destination.is_file():
        destination.unlink()
        destination.mkdir(parents=True, exist_ok=True)

    destination.mkdir(parents=True, exist_ok=True)
    for child in source.iterdir():
        _merge_paths(child, destination / child.name)
    try:
        source.rmdir()
    except OSError:
        pass


def flatten_catalog_data_dir(output_dir: Path) -> None:
    """
    Move Nautilus catalog contents up one level so `data/` is removed.
    """
    data_dir = output_dir / "data"
    if not data_dir.exists():
        return

    logger.info("Flattening catalog layout: moving %s into %s", data_dir, output_dir)
    entries = sorted(data_dir.iterdir(), key=lambda p: p.name.lower())
    for entry in entries:
        _merge_paths(entry, output_dir / entry.name)

    shutil.rmtree(data_dir, ignore_errors=True)
    logger.info("Removed intermediate 'data' directory; catalog root is now %s", output_dir)


def resolve_venue(symbol: str | None, default: str = "NSE") -> str:
    if not symbol:
        return default
    return VENUE_OVERRIDES.get(symbol.upper(), default)


def normalize_contract_symbol(symbol: str | None) -> str:
    """
    Standardize NSE contract symbols (strip suffixes like .NSE and uppercase).
    """
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


def discover_symbols(input_dir: Path) -> list[str]:
    """
    Discover available symbols by scanning known directory layouts and parquet filenames.
    """
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


def resolve_symbol_list(input_dir: Path, symbols: list[str] | None) -> list[str]:
    """
    Resolve user-provided symbols or auto-discover them when requested.
    """
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


def gather_local_futures_files(input_dir: Path, symbol: str) -> list[Path]:
    """
    Collect futures parquet files for a symbol from the local filesystem.
    """
    symbol_lower = symbol.lower()
    futures_root = input_dir / "futures" / symbol_lower

    def sorted_parquet(paths: list[Path]) -> list[Path]:
        return sorted(p for p in paths if p.is_file() and p.suffix == ".parquet")

    all_files: list[Path] = []
    if futures_root.exists():
        all_files = sorted_parquet(list(futures_root.rglob("*.parquet")))

    if not all_files:
        all_files = sorted_parquet(
            [
                path
                for path in input_dir.rglob("*.parquet")
                if path.stem.lower().startswith(f"{symbol_lower}_future_")
            ]
        )

    if not all_files:
        return []

    dated_prefix = f"{symbol_lower}_future_"
    dated_files = [path for path in all_files if path.stem.lower().startswith(dated_prefix)]
    return dated_files if dated_files else all_files


def gather_local_option_files(input_dir: Path, symbol: str) -> list[Path]:
    """
    Collect option parquet files (call + put) for a symbol from the local filesystem.
    """
    symbol_lower = symbol.lower()
    option_dirs = find_option_directories(input_dir, symbol)
    option_files: list[Path] = []

    for directory in option_dirs:
        option_files.extend(path for path in directory.rglob("*.parquet") if path.is_file())

    if not option_files:
        option_files = [
            path
            for path in input_dir.rglob("*.parquet")
            if path.stem.lower().startswith(f"{symbol_lower}_call_")
            or path.stem.lower().startswith(f"{symbol_lower}_put_")
        ]

    if not option_files:
        return []

    call_prefix = f"{symbol_lower}_call_"
    put_prefix = f"{symbol_lower}_put_"
    dated_files = [
        path
        for path in option_files
        if path.stem.lower().startswith(call_prefix) or path.stem.lower().startswith(put_prefix)
    ]
    return sorted(dated_files if dated_files else option_files)


def create_spaces_client(
    endpoint: str,
    region: str,
    access_key: str,
    secret_key: str,
    connect_timeout: int = 60,
    read_timeout: int = 300,
    max_attempts: int = 5,
):
    """
    Create a boto3 client configured for DigitalOcean Spaces uploads.
    """
    if not access_key or not secret_key:
        raise ValueError("DigitalOcean Spaces access key and secret key are required for uploads.")

    config = BotoConfig(
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        retries={
            "max_attempts": max_attempts,
            "mode": "adaptive",
        },
    )

    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=config,
    )


def upload_catalog_to_spaces(
    catalog_dir: Path,
    bucket: str,
    prefix: str,
    client,
    max_workers: int = 8,
) -> None:
    """
    Upload the generated catalog directory to DigitalOcean Spaces.
    """
    catalog_dir = catalog_dir.resolve()
    if not catalog_dir.exists():
        raise FileNotFoundError(f"Catalog directory {catalog_dir} does not exist.")

    files = [path for path in catalog_dir.rglob("*") if path.is_file()]
    if not files:
        logger.warning("Catalog directory %s is empty; nothing to upload.", catalog_dir)
        return

    prefix = prefix.strip().strip("/")

    def destination_key(local_path: Path) -> str:
        relative = local_path.relative_to(catalog_dir).as_posix()
        return f"{prefix}/{relative}" if prefix else relative

    logger.info(
        "Uploading %s files to DigitalOcean Spaces bucket '%s' with prefix '%s'",
        len(files),
        bucket,
        prefix or "(root)",
    )

    uploaded = 0
    failed = 0

    def upload_one(path: Path):
        nonlocal uploaded, failed
        key = destination_key(path)
        try:
            client.upload_file(str(path), bucket, key)
            uploaded += 1
            if uploaded % 100 == 0 or uploaded == len(files):
                logger.info("Uploaded %s/%s files to Spaces", uploaded, len(files))
        except Exception as exc:
            failed += 1
            logger.error("Failed to upload %s to %s: %s", path, key, exc)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(upload_one, path) for path in files]
        for future in as_completed(futures):
            # Exceptions already handled inside upload_one
            future.result()

    if failed:
        logger.warning("Upload completed with %s failures", failed)
    else:
        logger.info("✅ Upload to DigitalOcean Spaces completed successfully.")


def upload_catalog_with_rclone(
    catalog_dir: Path,
    remote: str,
    bucket: str,
    prefix: str,
    transfers: int = 8,
    binary: str = "rclone",
    extra_args: str | None = None,
) -> None:
    """
    Upload catalog using rclone for faster parallel transfers.
    """
    catalog_dir = catalog_dir.resolve()
    if not catalog_dir.exists():
        raise FileNotFoundError(f"Catalog directory {catalog_dir} does not exist.")

    if not remote:
        raise ValueError("rclone remote is required (use --rclone-remote or RCLONE_REMOTE).")

    if shutil.which(binary) is None:
        raise RuntimeError(f"rclone binary '{binary}' not found on PATH.")

    dest = f"{remote}:{bucket.strip('/')}"
    prefix = prefix.strip().strip("/")
    if prefix:
        dest = f"{dest}/{prefix}"

    cmd = [
        binary,
        "copy",
        str(catalog_dir),
        dest,
        "--transfers",
        str(transfers),
        "--checkers",
        str(max(transfers, 4)),
        "--progress",
    ]

    if extra_args:
        cmd.extend(shlex.split(extra_args))

    logger.info("Uploading catalog via rclone: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        logger.error("rclone upload failed:\nSTDOUT: %s\nSTDERR: %s", proc.stdout, proc.stderr)
        proc.check_returncode()
    else:
        logger.info("✅ rclone upload completed successfully.")


def normalize_price_columns(df: pd.DataFrame, columns: list[str]) -> bool:
    """
    Ensure price columns are floating point and scale paise-based data to rupees.

    Returns:
        True if scaling (÷100) was applied, otherwise False.
    """
    if df.empty:
        return False

    df[columns] = df[columns].astype(float)
    max_abs_value = df[columns].abs().max().max()

    if max_abs_value > 100_000:
        df[columns] = df[columns] / 100.0
        return True

    return False


def bars_to_quote_ticks(bars, instrument):
    """
    Convert Bar data to QuoteTicks for Greeks calculation.

    Creates QuoteTicks where bid=ask=close price.
    This is required for NautilusTrader Greeks calculator.
    """
    quote_ticks = []

    for bar in bars:
        # Create QuoteTick using close price as both bid and ask
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


def yyyymmdd_seconds_to_datetime(date_int: int | str, time_int: int | str) -> datetime:
    """
    Convert YYYYMMDD integer + seconds to datetime in UTC.
    
    Args:
        date_int: Date as YYYYMMDD (e.g., 20240102), YYMMDD, or string like 'DD/MM/YYYY'
        time_int: Time as seconds since midnight (e.g., 33300 = 09:15:00) or string like 'HH:MM:SS'
    
    Returns:
        datetime in UTC
    """
    # Parse date
    if isinstance(date_int, datetime):
        year, month, day = date_int.year, date_int.month, date_int.day
    elif isinstance(date_int, date):
        year, month, day = date_int.year, date_int.month, date_int.day
    else:
        if isinstance(date_int, float) and date_int.is_integer():
            date_int = int(date_int)
        date_parsed = False
        if isinstance(date_int, str):
            # Try parsing DD/MM/YYYY format
            if '/' in date_int:
                try:
                    dt = pd.to_datetime(date_int, dayfirst=True)
                    year, month, day = dt.year, dt.month, dt.day
                    date_parsed = True
                except:
                    # Fallback to removing separators
                    stripped = date_int.replace("-", "").replace("/", "").strip()
                    if not stripped:
                        raise ValueError(f"Unsupported date format: {date_int!r}")
                    try:
                        date_int = int(stripped)
                    except ValueError as exc:
                        raise ValueError(f"Unsupported date format: {date_int!r}") from exc
            else:
                stripped = date_int.replace("-", "").strip()
                if not stripped:
                    raise ValueError(f"Unsupported date format: {date_int!r}")
                try:
                    date_int = int(stripped)
                except ValueError as exc:
                    raise ValueError(f"Unsupported date format: {date_int!r}") from exc

        if not date_parsed and isinstance(date_int, int):
            if date_int >= 10_000_000:
                year = date_int // 10000
                month = (date_int % 10000) // 100
                day = date_int % 100
            elif date_int >= 100_000:
                # Handle YYMMDD sources (assume 2000s)
                year = 2000 + (date_int // 10000)
                month = (date_int % 10000) // 100
                day = date_int % 100
            else:
                raise ValueError(f"Unsupported date format: {date_int}")
        elif not isinstance(date_int, str):
            raise TypeError(f"Unsupported date type: {type(date_int)}")
    
    # Parse time - handle both integer seconds and HH:MM:SS strings
    if isinstance(time_int, str) and ':' in time_int:
        # Convert HH:MM:SS to seconds
        parts = time_int.split(':')
        time_int = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    elif isinstance(time_int, str):
        time_int = int(time_int)
    
    hours = time_int // 3600
    minutes = (time_int % 3600) // 60
    seconds = time_int % 60
    
    # Create IST datetime (naive)
    ist_dt = datetime(year, month, day, hours, minutes, seconds)
    
    # Convert to UTC
    utc_dt = ist_dt - IST_OFFSET
    
    return utc_dt


def vectorized_timestamp_conversion(df: pd.DataFrame) -> pd.Series:
    """
    FAST vectorized timestamp conversion (100x faster than row-by-row apply).
    Converts date (YYYYMMDD) + time (seconds) columns to UTC timestamps.
    """
    # Handle date column - convert to integer if needed
    dates = df['date'].copy()
    if dates.dtype == 'object':
        dates = pd.to_datetime(dates, dayfirst=True, errors='coerce').dt.strftime('%Y%m%d').astype(int)
    
    # Handle time column - convert to integer seconds if needed
    times = df['time'].copy()
    if times.dtype == 'object':
        # Check if format is HH:MM:SS
        if times.astype(str).str.contains(':', na=False).any():
            time_parts = times.astype(str).str.split(':')
            times = time_parts.str[0].astype(int) * 3600 + time_parts.str[1].astype(int) * 60 + time_parts.str[2].astype(int)
        else:
            times = pd.to_numeric(times, errors='coerce').astype(int)
    
    # Extract date components (vectorized)
    years = dates // 10000
    months = (dates % 10000) // 100
    days = dates % 100
    
    # Extract time components (vectorized)
    hours = times // 3600
    minutes = (times % 3600) // 60
    seconds = times % 60
    
    # Create IST datetime (vectorized) - much faster than row-by-row!
    ist_timestamps = pd.to_datetime({
        'year': years,
        'month': months,
        'day': days,
        'hour': hours,
        'minute': minutes,
        'second': seconds
    })
    
    # Convert IST to UTC (vectorized)
    utc_timestamps = ist_timestamps - IST_OFFSET
    
    return utc_timestamps


def transform_index_bars(
    input_dir: Path,
    catalog: ParquetDataCatalog,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None
) -> int:
    """
    Transform index data to Nautilus Bar format (OFFICIAL PATTERN).
    
    Args:
        input_dir: Directory containing raw parquet files
        catalog: Nautilus ParquetDataCatalog instance
        symbol: Symbol name (e.g., "NIFTY", "BANKNIFTY")
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
    
    Returns:
        Number of bars created
    """
    logger.info(f"Transforming {symbol} index bars...")
    
    # Resolve index directory (supports legacy 'index' and new 'cash' folders)
    symbol_dir = find_index_directory(input_dir, symbol)
    if not symbol_dir:
        logger.warning(
            f"No index/cash directory found for {symbol} under {input_dir} "
            f"(checked: {', '.join(INDEX_DIR_CANDIDATES)})"
        )
        return 0

    logger.info(f"Using index source directory: {symbol_dir}")

    # Find all parquet files for this symbol
    parquet_files = list(symbol_dir.rglob("*.parquet"))
    
    if not parquet_files:
        logger.warning(f"No parquet files found in {symbol_dir}")
        return 0
    
    # Read all files into one DataFrame
    dfs = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Error reading {file}: {e}")
            continue
    
    if not dfs:
        logger.error("No data loaded")
        return 0
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Convert date + time to datetime timestamp
    combined_df['timestamp'] = combined_df.apply(
        lambda row: yyyymmdd_seconds_to_datetime(row['date'], row['time']),
        axis=1
    )
    
    logger.info(f"Data range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
    
    # Filter by date range (account for IST->UTC conversion: IST dates start at UTC-5:30)
    start = pd.to_datetime(start_date) - pd.Timedelta(hours=6) if start_date else None
    end = pd.to_datetime(end_date) + pd.Timedelta(days=1) if end_date else None

    if start is not None:
        combined_df = combined_df[combined_df['timestamp'] >= start]
    if end is not None:
        combined_df = combined_df[combined_df['timestamp'] < end]
    
    if combined_df.empty:
        logger.warning(f"No data in date range {describe_date_range(start_date, end_date)}")
        return 0

    if "volume" not in combined_df.columns:
        logger.info("No volume column present for %s index data; defaulting to zeros.", symbol)
        combined_df["volume"] = 0.0
    
    # OFFICIAL PATTERN: Prepare DataFrame for BarDataWrangler
    # Required: columns ['open', 'high', 'low', 'close', 'volume'] with 'timestamp' as INDEX
    bar_df = combined_df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()

    if normalize_price_columns(bar_df, ["open", "high", "low", "close"]):
        logger.info("Scaled %s index prices from paise to rupees.", symbol)

    # Data quality fixes
    initial_count = len(bar_df)
    bar_df = bar_df.dropna(subset=["open", "high", "low", "close"])
    dropped_nan = initial_count - len(bar_df)
    if dropped_nan:
        logger.warning("%s index: Dropped %s rows with NaN OHLC values.", symbol, dropped_nan)

    bar_df["volume"] = bar_df["volume"].fillna(0).clip(lower=0)
    bar_df["high"] = bar_df[["high", "open", "close"]].max(axis=1)
    bar_df["low"] = bar_df[["low", "open", "close"]].min(axis=1)

    # Prices are now normalized to rupees with 0.05 tick size

    bar_df = bar_df.set_index('timestamp')  # CRITICAL: Set timestamp as index!
    bar_df = bar_df.sort_index()  # Sort by timestamp
    
    # Create instrument
    venue_code = resolve_venue(symbol, default="NSE")

    instrument_id = InstrumentId(
        symbol=Symbol(f"{symbol}-INDEX"),
        venue=Venue(venue_code)
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
    
    # Create bar type
    bar_type = BarType.from_str(f"{instrument_id}{BAR_TYPE_SUFFIX}")
    
    # OFFICIAL PATTERN: Use BarDataWrangler
    wrangler = BarDataWrangler(bar_type, instrument)
    bars = wrangler.process(
        data=bar_df,
        default_volume=0.0,  # Index data has no real volume
        ts_init_delta=0
    )
    
    # OFFICIAL PATTERN: Write to catalog
    catalog.write_data([instrument])  # Write instrument first
    catalog.write_data(bars, skip_disjoint_check=True)  # Skip check for overlapping data

    # Generate and write QuoteTicks for Greeks calculation
    quote_ticks = bars_to_quote_ticks(bars, instrument)
    catalog.write_data(quote_ticks, skip_disjoint_check=True)
    logger.info(f"✅ {symbol}: Created {len(bars):,} bars + {len(quote_ticks):,} QuoteTicks")

    return len(bars)


def transform_futures_bars(
    input_dir: Path,
    catalog: ParquetDataCatalog,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
    io_workers: int = 8,
) -> tuple[int, None]:
    """
    Transform local futures parquet files to Nautilus Bar format + FutureOI records.
    """
    logger.info("Transforming %s futures bars from local parquet files...", symbol)

    parquet_files = gather_local_futures_files(input_dir, symbol)
    if not parquet_files:
        logger.warning("No futures parquet files found for %s under %s", symbol, input_dir)
        return 0, None

    logger.info("Processing %s futures files located on disk", len(parquet_files))
    dfs = read_parquet_batch_local(parquet_files, max_workers=io_workers)
    if not dfs:
        logger.warning("Failed to load any futures data for %s", symbol)
        return 0, None

    combined_df = pd.concat(dfs, ignore_index=True)
    if combined_df.empty:
        logger.warning("Combined futures dataframe is empty for %s", symbol)
        return 0, None

    required_columns = {"date", "time", "open", "high", "low", "close", "volume"}
    missing = required_columns - set(combined_df.columns)
    if missing:
        raise ValueError(f"{symbol} futures missing required columns: {missing}")

    combined_df["timestamp"] = vectorized_timestamp_conversion(combined_df)
    logger.info(
        "Futures data range for %s: %s to %s",
        symbol,
        combined_df["timestamp"].min(),
        combined_df["timestamp"].max(),
    )

    start = pd.to_datetime(start_date) - pd.Timedelta(hours=6) if start_date else None
    end = pd.to_datetime(end_date) + pd.Timedelta(days=1) if end_date else None
    if start is not None:
        combined_df = combined_df[combined_df["timestamp"] >= start]
    if end is not None:
        combined_df = combined_df[combined_df["timestamp"] < end]

    if combined_df.empty:
        logger.warning(
            "No futures data in date range %s for %s",
            describe_date_range(start_date, end_date),
            symbol,
        )
        return 0, None

    bar_df = combined_df[
        ["timestamp", "open", "high", "low", "close", "volume"]
    ].copy()

    if normalize_price_columns(bar_df, ["open", "high", "low", "close"]):
        logger.info("Scaled %s futures prices from paise to rupees.", symbol)

    bar_df["volume"] = bar_df["volume"].clip(lower=0)
    bar_df["high"] = bar_df[["high", "close"]].max(axis=1)
    bar_df["low"] = bar_df[["low", "close"]].min(axis=1)

    bar_df = bar_df.set_index("timestamp").sort_index()

    venue_code = resolve_venue(symbol, default="NSE")

    instrument = create_futures_contract(
        symbol=f"{symbol}-I",
        expiry_date="continuous",
        underlying=symbol,
        venue=venue_code,
    )

    bar_type = BarType.from_str(f"{instrument.id}{BAR_TYPE_SUFFIX}")
    wrangler = BarDataWrangler(bar_type, instrument)
    bars = wrangler.process(bar_df)

    quote_ticks = bars_to_quote_ticks(bars, instrument)

    oi_data_list = []
    if "oi" in combined_df.columns:
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
                ts_init=ts_ns,
            )
            oi_data_list.append(oi_data)
        if not oi_data_list:
            logger.info("No valid FUTURE OI values found for %s; skipping FutureOI output.", symbol)
    else:
        logger.info("No 'oi' column present in futures data for %s; skipping FutureOI output.", symbol)

    catalog.write_data([instrument])
    catalog.write_data(bars, skip_disjoint_check=True)
    catalog.write_data(quote_ticks, skip_disjoint_check=True)

    if oi_data_list:
        oi_data_list.sort(key=lambda x: x.ts_init)
        catalog.write_data(oi_data_list, skip_disjoint_check=True)
        logger.info("✅ Saved %s FutureOI records", len(oi_data_list))

    logger.info(
        "✅ %s futures: Created %s bars + %s QuoteTicks",
        symbol,
        len(bars),
        len(quote_ticks),
    )
    return len(bars), None


def read_parquet_batch_local(files: List[Path], max_workers: int = 8) -> List[pd.DataFrame]:
    """
    Read multiple parquet files from disk in parallel.
    """
    if not files:
        return []

    dfs: list[pd.DataFrame] = []
    total = len(files)
    completed = 0

    def read_one(path: Path) -> pd.DataFrame:
        return pd.read_parquet(path)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_path = {executor.submit(read_one, path): path for path in files}
        for future in as_completed(future_to_path):
            path = future_to_path[future]
            completed += 1
            try:
                df = future.result()
                if not df.empty:
                    dfs.append(df)
            except Exception as exc:
                logger.warning("Error reading %s: %s", path, exc)
            if completed % 50 == 0 or completed == total:
                logger.info(
                    "  Progress: %s/%s local files read (%s%%)",
                    completed,
                    total,
                    100 * completed // total,
                )

    return dfs


def transform_options_bars(
    input_dir: Path,
    catalog: ParquetDataCatalog,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
    batch_size: int = 100,
    io_workers: int = 20,
) -> int:
    """
    Transform local options parquet files to Nautilus Bar + OptionOI data.
    """
    logger.info("Transforming %s options bars from local parquet files...", symbol)

    parquet_files = gather_local_option_files(input_dir, symbol)
    if not parquet_files:
        logger.warning("No options parquet files found for %s under %s", symbol, input_dir)
        return 0

    logger.info("Processing %s option parquet files located on disk", len(parquet_files))
    all_dfs = read_parquet_batch_local(parquet_files, max_workers=io_workers)
    if not all_dfs:
        logger.warning("No option data loaded for %s", symbol)
        return 0

    total_bars = 0
    total_quote_ticks = 0
    scaled_option_prices_logged = False

    instruments_batch: list = []
    bars_batch: list = []
    quote_ticks_batch: list = []
    oi_data_batch: list = []

    def flush_batches():
        nonlocal instruments_batch, bars_batch, quote_ticks_batch, oi_data_batch
        if not instruments_batch:
            return
        if bars_batch:
            bars_batch.sort(key=lambda bar: bar.ts_init)
        if quote_ticks_batch:
            quote_ticks_batch.sort(key=lambda tick: tick.ts_init)
        catalog.write_data(instruments_batch, skip_disjoint_check=True)
        catalog.write_data(bars_batch, skip_disjoint_check=True)
        catalog.write_data(quote_ticks_batch, skip_disjoint_check=True)
        if oi_data_batch:
            oi_data_batch.sort(key=lambda x: x.ts_init)
            catalog.write_data(oi_data_batch, skip_disjoint_check=True)
        instruments_batch.clear()
        bars_batch.clear()
        quote_ticks_batch.clear()
        oi_data_batch.clear()

    for df in all_dfs:
        if df.empty or "symbol" not in df.columns:
            continue

        missing = {"date", "time", "open", "high", "low", "close", "volume"} - set(df.columns)
        if missing:
            logger.warning("Skipping dataframe missing required columns %s for %s", missing, symbol)
            continue

        df["timestamp"] = vectorized_timestamp_conversion(df)
        df["symbol"] = df["symbol"].astype(str).str.strip()

        start = pd.to_datetime(start_date) - pd.Timedelta(hours=6) if start_date else None
        end = pd.to_datetime(end_date) + pd.Timedelta(days=1) if end_date else None
        if start is not None:
            df = df[df["timestamp"] >= start]
        if end is not None:
            df = df[df["timestamp"] < end]
        if df.empty:
            continue

        for original_symbol, group in df.groupby("symbol"):
            try:
                normalized_symbol = normalize_contract_symbol(original_symbol)
                if not normalized_symbol:
                    logger.warning("Skipping option data with empty symbol")
                    continue

                group_sorted = group.sort_values("timestamp")
                bar_df = group_sorted[
                    ["timestamp", "open", "high", "low", "close", "volume"]
                ].copy()

                bar_df["volume"] = bar_df["volume"].clip(lower=0)
                bar_df["high"] = bar_df[["high", "close", "open"]].max(axis=1)
                bar_df["low"] = bar_df[["low", "close", "open"]].min(axis=1)

                if normalize_price_columns(bar_df, ["open", "high", "low", "close"]):
                    if not scaled_option_prices_logged:
                        logger.info("Scaled %s options prices from paise to rupees.", symbol)
                        scaled_option_prices_logged = True

                bar_df = bar_df.set_index("timestamp").sort_index()

                expiry_hint = group_sorted["expiry"].iloc[0] if "expiry" in group_sorted.columns and not group_sorted["expiry"].empty else None
                strike_hint = group_sorted["strike"].iloc[0] if "strike" in group_sorted.columns and not group_sorted["strike"].empty else None

                try:
                    parsed = parse_nse_option_symbol(
                        normalized_symbol,
                        expiry_hint=expiry_hint,
                        strike_hint=strike_hint,
                    )
                except Exception as parse_error:
                    logger.error(
                        "Skipping option %s (normalized %s): %s",
                        original_symbol,
                        normalized_symbol,
                        parse_error,
                    )
                    continue

                venue_code = resolve_venue(parsed["underlying"], default="NSE")

                instrument = create_options_contract(
                    symbol=normalized_symbol,
                    underlying=parsed["underlying"],
                    strike=parsed["strike"],
                    expiry=parsed["expiry"],
                    option_kind=parsed["option_type"],
                    venue=venue_code,
                )

                bar_type = BarType.from_str(f"{instrument.id}{BAR_TYPE_SUFFIX}")
                wrangler = BarDataWrangler(bar_type, instrument)
                bars = wrangler.process(bar_df)

                instruments_batch.append(instrument)
                bars_batch.extend(bars)

                quote_ticks = bars_to_quote_ticks(bars, instrument)
                quote_ticks_batch.extend(quote_ticks)

                if "oi" in group_sorted.columns:
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
                            ts_init=ts_ns,
                        )
                        oi_data_batch.append(oi_data)

                total_bars += len(bars)
                total_quote_ticks += len(quote_ticks)

                if len(instruments_batch) >= batch_size:
                    flush_batches()

            except Exception as exc:
                logger.warning("Error processing option %s: %s", original_symbol, exc)
                continue

    flush_batches()

    logger.info(
        "✅ %s options: Created %s bars + %s QuoteTicks",
        symbol,
        total_bars,
        total_quote_ticks,
    )
    return total_bars


def main():
    parser = argparse.ArgumentParser(
        description="Transform local NSE futures/options parquet data to a Nautilus catalog"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Root directory containing raw parquet data (expects futures/ and options/ subfolders)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd(),
        help="Output directory for Nautilus catalog (default: current working directory)"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols to transform (default: auto-discover from local directories)"
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
        "--parallel-symbols",
        type=int,
        default=3,
        help="Number of symbols to process in parallel (default: 3)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for catalog writes in options processing (default: 100)"
    )
    parser.add_argument(
        "--io-workers",
        type=int,
        default=8,
        help="Number of parallel workers when reading parquet files from disk"
    )
    parser.add_argument(
        "--keep-data-subdir",
        action="store_true",
        help="Preserve the default Nautilus 'data/' subdirectory layout (default: flatten output)."
    )
    parser.set_defaults(upload_to_spaces=True)
    parser.add_argument(
        "--upload-to-spaces",
        action="store_true",
        dest="upload_to_spaces",
        help="Upload the generated catalog to DigitalOcean Spaces after processing (default: enabled)"
    )
    parser.add_argument(
        "--no-upload-to-spaces",
        action="store_false",
        dest="upload_to_spaces",
        help="Skip uploading the catalog to DigitalOcean Spaces"
    )
    parser.add_argument(
        "--spaces-bucket",
        type=str,
        default=os.environ.get("DO_BUCKET", "historical-db-1min"),
        help="DigitalOcean Spaces bucket name (default: env DO_BUCKET or 'historical-db-1min')"
    )
    parser.add_argument(
        "--spaces-prefix",
        type=str,
        default=os.environ.get("DO_PREFIX", "sample_catalog"),
        help="Prefix within the bucket for uploads (default: env DO_PREFIX or 'sample_catalog')"
    )
    parser.add_argument(
        "--spaces-endpoint",
        type=str,
        default=os.environ.get("DO_ENDPOINT", "https://blr1.digitaloceanspaces.com"),
        help="DigitalOcean Spaces endpoint URL"
    )
    parser.add_argument(
        "--spaces-region",
        type=str,
        default=os.environ.get("DO_REGION", "blr1"),
        help="DigitalOcean Spaces region"
    )
    parser.add_argument(
        "--spaces-access-key",
        type=str,
        default=os.environ.get("DO_ACCESS_KEY", "DO00CDX8Z7BFTQJ9W2AZ"),
        help="DigitalOcean Spaces access key (default: DO_ACCESS_KEY env var or built-in demo key)"
    )
    parser.add_argument(
        "--spaces-secret-key",
        type=str,
        default=os.environ.get("DO_SECRET_KEY", "kR159s1x7Xjfd6RhVIyw8X34UGLIFKSzP7/0fG6Yt9I"),
        help="DigitalOcean Spaces secret key (default: DO_SECRET_KEY env var or built-in demo key)"
    )
    parser.add_argument(
        "--spaces-workers",
        type=int,
        default=8,
        help="Parallel upload workers when pushing to Spaces (default: 8)"
    )
    parser.add_argument(
        "--upload-method",
        choices=["boto3", "rclone"],
        default=os.environ.get("UPLOAD_METHOD", "boto3"),
        help="Upload implementation to use (default: boto3, set UPLOAD_METHOD env)"
    )
    parser.add_argument(
        "--rclone-remote",
        type=str,
        default=os.environ.get("RCLONE_REMOTE"),
        help="rclone remote name configured for Spaces (required when --upload-method=rclone)"
    )
    parser.add_argument(
        "--rclone-binary",
        type=str,
        default=os.environ.get("RCLONE_BIN", "rclone"),
        help="rclone executable to invoke (default: 'rclone' or RCLONE_BIN env)"
    )
    parser.add_argument(
        "--rclone-extra-args",
        type=str,
        default=os.environ.get("RCLONE_EXTRA_ARGS", ""),
        help="Additional CLI args passed to rclone copy (quoted string)"
    )
    
    args = parser.parse_args()

    if not args.input_dir.exists():
        logger.error("Input directory %s does not exist.", args.input_dir)
        sys.exit(1)

    if args.upload_to_spaces and not args.spaces_bucket:
        logger.error("--upload-to-spaces requires --spaces-bucket (or DO_BUCKET env var).")
        sys.exit(1)
    
    symbols = resolve_symbol_list(args.input_dir, args.symbols)
    date_range_label = describe_date_range(args.start_date, args.end_date)
    
    logger.info("="*80)
    logger.info("NAUTILUS DATA TRANSFORMATION (LOCAL PARQUET)")
    logger.info("Following: nautilus_trader/examples/backtest/ patterns")
    logger.info("="*80)
    logger.info("Source (local): %s", args.input_dir)
    logger.info("Output: %s", args.output_dir)
    logger.info("Symbols: %s", symbols)
    logger.info("Date range: %s", date_range_label)
    logger.info("="*80)
    logger.info("⚡ PERFORMANCE OPTIMIZATIONS ENABLED:")
    logger.info("  - Parallel symbol processing: %s workers", args.parallel_symbols)
    logger.info("  - Parallel file loads per symbol: %s workers", args.io_workers)
    logger.info("  - Batched catalog writes: %s instruments per batch (options)", args.batch_size)
    logger.info("="*80)

    catalog = ParquetDataCatalog(path=str(args.output_dir))
    data_root = ensure_catalog_structure(args.output_dir)
    logger.info("Ensured catalog structure under %s", data_root)
    
    total_bars = 0
    
    # Skip index/cash transformation - only process futures and options
    
    # Transform futures data (parallel processing)
    logger.info("\n" + "="*80)
    logger.info("TRANSFORMING FUTURES DATA")
    logger.info("⚡ Using %s parallel workers for symbol processing", args.parallel_symbols)
    logger.info("="*80)
    
    def process_futures_symbol(symbol):
        """Process a single symbol's futures data."""
        try:
            count, _ = transform_futures_bars(
                args.input_dir,
                catalog,
                symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                io_workers=args.io_workers,
            )
            return count
        except Exception as e:
            logger.error(f"Error transforming {symbol} futures: {e}", exc_info=True)
            return 0
    
    # Process futures in parallel
    with ThreadPoolExecutor(max_workers=args.parallel_symbols) as executor:
        futures_results = {executor.submit(process_futures_symbol, symbol): symbol for symbol in symbols}
        for future in as_completed(futures_results):
            symbol = futures_results[future]
            try:
                count = future.result()
                total_bars += count
                logger.info(f"✅ Completed {symbol} futures: {count:,} bars")
            except Exception as e:
                logger.error(f"Error processing {symbol} futures: {e}", exc_info=True)
    
    # Transform options data (parallel processing)
    logger.info("\n" + "="*80)
    logger.info("TRANSFORMING OPTIONS DATA")
    logger.info("⚡ Using %s parallel workers for symbol processing", args.parallel_symbols)
    logger.info("="*80)
    
    def process_options_symbol(symbol):
        """Process a single symbol's options data."""
        try:
            count = transform_options_bars(
                args.input_dir,
                catalog,
                symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                batch_size=args.batch_size,
                io_workers=args.io_workers,
            )
            return count
        except Exception as e:
            logger.error(f"Error transforming {symbol} options: {e}", exc_info=True)
            return 0
    
    # Process options in parallel
    with ThreadPoolExecutor(max_workers=args.parallel_symbols) as executor:
        options_results = {executor.submit(process_options_symbol, symbol): symbol for symbol in symbols}
        for future in as_completed(options_results):
            symbol = options_results[future]
            try:
                count = future.result()
                total_bars += count
                logger.info(f"✅ Completed {symbol} options: {count:,} bars")
            except Exception as e:
                logger.error(f"Error processing {symbol} options: {e}", exc_info=True)
    
    if not args.keep_data_subdir:
        flatten_catalog_data_dir(args.output_dir)

    layout_root = args.output_dir if not args.keep_data_subdir else args.output_dir / "data"

    # Summary
    print("\n" + "="*80)
    print("TRANSFORMATION COMPLETE")
    print("="*80)
    print(f"Total bars created: {total_bars:,}")
    print(f"Catalog location: {args.output_dir}")
    print(f"Date range processed: {date_range_label}")
    print("="*80)
    print("\nData structure:")
    print(f"  Bar data: {layout_root}/bar/")
    print(f"  QuoteTick data: {layout_root}/quote_tick/")
    print(f"  FutureOI data: {layout_root}/custom_future_oi/")
    print(f"  OptionOI data: {layout_root}/custom_option_oi/")
    print(f"  Instruments: {layout_root}/futures_contract/ and {layout_root}/option_contract/")
    print("\nNext steps:")
    print("  1. Verify data: catalog.bars()")
    print("  2. Query FutureOI: catalog.generic_data(FutureOI)")
    print("  3. Query OptionOI: catalog.generic_data(OptionOI)")
    print("  4. Run backtest with transformed data")
    print("="*80)

    if args.upload_to_spaces:
        logger.info(
            "Starting upload to DigitalOcean Spaces bucket '%s' (prefix='%s') via %s",
            args.spaces_bucket,
            args.spaces_prefix,
            args.upload_method,
        )
        try:
            if args.upload_method == "rclone":
                upload_catalog_with_rclone(
                    catalog_dir=args.output_dir,
                    remote=args.rclone_remote,
                    bucket=args.spaces_bucket,
                    prefix=args.spaces_prefix,
                    transfers=args.spaces_workers,
                    binary=args.rclone_binary,
                    extra_args=args.rclone_extra_args,
                )
            else:
                spaces_client = create_spaces_client(
                    endpoint=args.spaces_endpoint,
                    region=args.spaces_region,
                    access_key=args.spaces_access_key,
                    secret_key=args.spaces_secret_key,
                )
                upload_catalog_to_spaces(
                    catalog_dir=args.output_dir,
                    bucket=args.spaces_bucket,
                    prefix=args.spaces_prefix,
                    client=spaces_client,
                    max_workers=args.spaces_workers,
                )
        except Exception as exc:
            logger.error("Failed to upload catalog to DigitalOcean Spaces: %s", exc, exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
