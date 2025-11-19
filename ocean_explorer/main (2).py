#!/usr/bin/env python3
"""
Official Nautilus Data Transformation Pattern

Follows the verified pattern from nautilus_trader/examples/backtest/
- example_01_load_bars_from_custom_csv/run_example.py
- example_04_using_data_catalog/run_example.py

Transforms NSE data (index, futures, options) to Nautilus catalog format.
"""

import sys
from pathlib import Path
from datetime import date, datetime, timedelta
import argparse
import logging

# Add repository root to path so package imports resolve when run as a script
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.data import BarType, QuoteTick
from nautilus_trader.model.instruments import Equity, OptionContract, FuturesContract
from nautilus_trader.model.objects import Price, Quantity, Currency
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import BarDataWrangler, QuoteTickDataWrangler

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


def ensure_catalog_structure(root: Path) -> Path:
    """
    Ensure Nautilus catalog subdirectories exist (creates them if missing).
    """
    data_root = root / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    for subdir in CATALOG_DATA_SUBDIRS:
        (data_root / subdir).mkdir(parents=True, exist_ok=True)
    return data_root


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


def yyyymmdd_seconds_to_datetime(date_int: int, time_int: int) -> datetime:
    """
    Convert YYYYMMDD integer + seconds to datetime in UTC.
    
    Args:
        date_int: Date as YYYYMMDD (e.g., 20240102) or YYMMDD (e.g., 240102 / 251001)
        time_int: Time as seconds since midnight (e.g., 33300 = 09:15:00)
    
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
            # Handle YYMMDD sources (assume 2000s)
            year = 2000 + (date_int // 10000)
            month = (date_int % 10000) // 100
            day = date_int % 100
        else:
            raise ValueError(f"Unsupported date format: {date_int}")
    
    # Parse time
    hours = time_int // 3600
    minutes = (time_int % 3600) // 60
    seconds = time_int % 60
    
    # Create IST datetime (naive)
    ist_dt = datetime(year, month, day, hours, minutes, seconds)
    
    # Convert to UTC
    utc_dt = ist_dt - IST_OFFSET
    
    return utc_dt


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
    
    # Create bar type
    bar_type = BarType.from_str(f"{instrument_id}-1-MINUTE-LAST-EXTERNAL")
    
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
    output_dir: Path = None
) -> tuple[int, None]:
    """
    Transform futures data to Nautilus Bar format + separate OI DataFrame.
    
    Returns:
        (bar_count, oi_dataframe)
    """
    logger.info(f"Transforming {symbol} futures bars...")
    
    symbol_dir = input_dir / "futures" / symbol.lower()
    parquet_files = list(symbol_dir.rglob("*.parquet"))

    if not parquet_files:
        logger.warning(f"No parquet files found in {symbol_dir}")
        return 0, pd.DataFrame()

    # CRITICAL: Only use dated files (nifty_future_YYYYMMDD.parquet) which are in RUPEES
    # Exclude data.parquet (in paise) and futures_data.parquet (corrupt)
    dated_files = [f for f in parquet_files if f.stem.startswith(f"{symbol.lower()}_future_")]

    if not dated_files:
        logger.warning(f"No dated futures files found in {symbol_dir}")
        return 0, pd.DataFrame()

    logger.info(f"Using {len(dated_files)} dated futures files (already in rupees)")

    dfs = []
    for file in dated_files:
        try:
            df = pd.read_parquet(file)
            # Handle mixed date formats
            if df['date'].dtype == 'object':
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d').astype(int)
            # Ensure time is int
            if df['time'].dtype == 'object':
                df['time'] = df['time'].astype(int)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Error reading {file}: {e}")
            continue
    
    if not dfs:
        return 0, pd.DataFrame()
    
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Convert to timestamp
    combined_df['timestamp'] = combined_df.apply(
        lambda row: yyyymmdd_seconds_to_datetime(row['date'], row['time']),
        axis=1
    )
    
    logger.info(f"Futures data range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
    
    # Filter by date range (account for IST->UTC conversion)
    start = pd.to_datetime(start_date) - pd.Timedelta(hours=6) if start_date else None
    end = pd.to_datetime(end_date) + pd.Timedelta(days=1) if end_date else None

    if start is not None:
        combined_df = combined_df[combined_df['timestamp'] >= start]
    if end is not None:
        combined_df = combined_df[combined_df['timestamp'] < end]
    
    if combined_df.empty:
        logger.warning(f"No futures data in date range {describe_date_range(start_date, end_date)} for {symbol}")
        return 0, pd.DataFrame()
    
    # Prepare for BarDataWrangler (OHLCV only, NO OI!)
    bar_df = combined_df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()

    if normalize_price_columns(bar_df, ["open", "high", "low", "close"]):
        logger.info("Scaled %s futures prices from paise to rupees.", symbol)

    # Data quality fixes
    bar_df['volume'] = bar_df['volume'].clip(lower=0)  # Handle negative volumes
    
    # Fix invalid OHLC relationships (Nautilus validates: high >= close, low <= close)
    bar_df['high'] = bar_df[['high', 'close']].max(axis=1)
    bar_df['low'] = bar_df[['low', 'close']].min(axis=1)
    
    bar_df = bar_df.set_index('timestamp')
    bar_df = bar_df.sort_index()
    
    # Create FuturesContract (use proper Nautilus instrument type)
    instrument = create_futures_contract(
        symbol=f"{symbol}-I",  # -I for continuous futures
        expiry_date="continuous",  # Continuous contract
        underlying=symbol,
        venue="NSE"
    )
    
    bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-LAST-EXTERNAL")
    
    # Create bars
    wrangler = BarDataWrangler(bar_type, instrument)
    bars = wrangler.process(bar_df)
    
    # Write to catalog
    catalog.write_data([instrument])
    catalog.write_data(bars, skip_disjoint_check=True)

    # Generate and write QuoteTicks for Greeks calculation
    quote_ticks = bars_to_quote_ticks(bars, instrument)
    catalog.write_data(quote_ticks, skip_disjoint_check=True)

    # Create FutureOI custom data (Arrow serialization registered)
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
                ts_init=ts_ns
            )
            oi_data_list.append(oi_data)
        if not oi_data_list:
            logger.info("No valid FUTURE OI values found for %s; skipping FutureOI output.", symbol)
    else:
        logger.info("No 'oi' column present in futures data for %s; skipping FutureOI output.", symbol)
    
    # Write FutureOI to catalog (Arrow registered)
    if oi_data_list:
        oi_data_list.sort(key=lambda x: x.ts_init)
        catalog.write_data(oi_data_list, skip_disjoint_check=True)
        logger.info(f"✅ Saved {len(oi_data_list):,} FutureOI records")
    
    logger.info(f"✅ {symbol} futures: Created {len(bars):,} bars + {len(quote_ticks):,} QuoteTicks")
    return len(bars), None  # No longer returning DataFrame


def transform_options_bars(
    input_dir: Path,
    catalog: ParquetDataCatalog,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None
) -> int:
    """
    Transform options data to Nautilus Bar format (OFFICIAL PATTERN).
    Note: Processes limited files to avoid memory issues.
    """
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
    
    # CRITICAL: Only use dated files (nifty_call/put_YYYYMMDD.parquet) which are in RUPEES
    # Similar to futures Bug #5 fix - dated files don't need paise conversion
    all_files: list[Path] = []
    for option_dir in option_dirs:
        all_files.extend(option_dir.rglob("*.parquet"))
    # Deduplicate while preserving order
    all_files = list(dict.fromkeys(all_files))

    # Filter for dated call/put files (already in rupees)
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

    # Process dated option files (already in rupees, no conversion needed)
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            
            if 'symbol' not in df.columns or df.empty:
                continue
            
            # Convert timestamp
            df['timestamp'] = df.apply(
                lambda row: yyyymmdd_seconds_to_datetime(row['date'], row['time']),
                axis=1
            )

            # Normalize symbol column for reliable grouping
            df['symbol'] = df['symbol'].astype(str).str.strip()
            
            # Filter by date (account for IST->UTC conversion)
            start = pd.to_datetime(start_date) - pd.Timedelta(hours=6) if start_date else None
            end = pd.to_datetime(end_date) + pd.Timedelta(days=1) if end_date else None

            if start is not None:
                df = df[df['timestamp'] >= start]
            if end is not None:
                df = df[df['timestamp'] < end]
            
            if df.empty:
                continue
            # Group by option symbol
            for original_symbol, group in df.groupby('symbol'):
                try:
                    normalized_symbol = normalize_contract_symbol(original_symbol)
                    if not normalized_symbol:
                        logger.warning("Skipping option data with empty symbol from %s", file)
                        continue

                    group_sorted = group.sort_values('timestamp')

                    bar_df = group_sorted[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()

                    # Data quality fixes
                    bar_df['volume'] = bar_df['volume'].clip(lower=0)
                    bar_df['high'] = bar_df[['high', 'close', 'open']].max(axis=1)
                    bar_df['low'] = bar_df[['low', 'close', 'open']].min(axis=1)

                    if normalize_price_columns(bar_df, ["open", "high", "low", "close"]):
                        if not scaled_option_prices_logged:
                            logger.info("Scaled %s options prices from paise to rupees.", symbol)
                            scaled_option_prices_logged = True
                    
                    bar_df = bar_df.set_index('timestamp')
                    bar_df = bar_df.sort_index()
                    
                    expiry_hint = None
                    if "expiry" in group_sorted.columns and not group_sorted["expiry"].empty:
                        expiry_hint = group_sorted["expiry"].iloc[0]

                    strike_hint = None
                    if "strike" in group_sorted.columns and not group_sorted["strike"].empty:
                        strike_hint = group_sorted["strike"].iloc[0]

                    # Create OptionContract (proper Nautilus instrument type)
                    try:
                        # Parse option symbol with dataset hints
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
                    
                    # Write to catalog
                    catalog.write_data([instrument])
                    catalog.write_data(bars, skip_disjoint_check=True)

                    # Generate and write QuoteTicks for Greeks calculation
                    quote_ticks = bars_to_quote_ticks(bars, instrument)
                    catalog.write_data(quote_ticks, skip_disjoint_check=True)

                    # Create OptionOI custom data (Arrow serialization registered)
                    if "oi" in group_sorted.columns:
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

                        # Write OptionOI to catalog
                        if oi_data_list:
                            oi_data_list.sort(key=lambda x: x.ts_init)
                            catalog.write_data(oi_data_list, skip_disjoint_check=True)


                    total_bars += len(bars)
                    total_quote_ticks += len(quote_ticks)
                    
                except Exception as e:
                    logger.warning(f"Error processing option {option_symbol}: {e}")
                    continue
                    
        except Exception as e:
            logger.warning(f"Error reading {file}: {e}")
            continue

    logger.info(f"✅ {symbol} options: Created {total_bars:,} bars + {total_quote_ticks:,} QuoteTicks")
    return total_bars


def main():
    parser = argparse.ArgumentParser(
        description="Transform NSE data to Nautilus catalog (Official Pattern)"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "original_source" / "raw_data",
        help="Input directory with raw data"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data",
        help="Output directory for Nautilus catalog"
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
    
    args = parser.parse_args()

    try:
        symbols = resolve_symbol_list(args.input_dir, args.symbols)
    except ValueError as exc:
        logger.error(exc)
        sys.exit(1)

    date_range_label = describe_date_range(args.start_date, args.end_date)
    
    logger.info("="*80)
    logger.info("NAUTILUS DATA TRANSFORMATION - OFFICIAL PATTERN")
    logger.info("Following: nautilus_trader/examples/backtest/ patterns")
    logger.info("="*80)
    logger.info(f"Input: {args.input_dir}")
    logger.info(f"Output: {args.output_dir}")
    logger.info(f"Symbols: {symbols}")
    logger.info(f"Date range: {date_range_label}")
    logger.info("="*80)
    
    # Create catalog and ensure expected directory structure exists
    catalog = ParquetDataCatalog(path=str(args.output_dir))
    data_root = ensure_catalog_structure(args.output_dir)
    logger.info("Ensured catalog structure under %s", data_root)
    
    total_bars = 0
    
    # Transform index data
    for symbol in symbols:
        try:
            count = transform_index_bars(
                args.input_dir,
                catalog,
                symbol,
                args.start_date,
                args.end_date
            )
            total_bars += count
        except Exception as e:
            logger.error(f"Error transforming {symbol} index: {e}", exc_info=True)
    
    # Transform futures data
    for symbol in symbols:
        try:
            count, _ = transform_futures_bars(  # Returns None now (FutureOI written directly)
                args.input_dir,
                catalog,
                symbol,
                args.start_date,
                args.end_date
            )
            total_bars += count
        except Exception as e:
            logger.error(f"Error transforming {symbol} futures: {e}", exc_info=True)
    
    # Transform options data
    for symbol in symbols:
        try:
            count = transform_options_bars(
                args.input_dir,
                catalog,
                symbol,
                args.start_date,
                args.end_date
            )
            total_bars += count
        except Exception as e:
            logger.error(f"Error transforming {symbol} options: {e}", exc_info=True)
    
    # Summary
    print("\n" + "="*80)
    print("TRANSFORMATION COMPLETE")
    print("="*80)
    print(f"Total bars created: {total_bars:,}")
    print(f"Catalog location: {args.output_dir}")
    print(f"Date range processed: {date_range_label}")
    print("="*80)
    print("\nData structure:")
    print(f"  Bar data: {args.output_dir}/bar/")
    print(f"  FutureOI data: Stored in Nautilus catalog")
    print(f"  Instruments: {args.output_dir}/instrument/")
    print("\nNext steps:")
    print("  1. Verify data: catalog.bars()")
    print("  2. Query FutureOI: catalog.generic_data(FutureOI)")
    print("  3. Run backtest with transformed data")
    print("="*80)


if __name__ == "__main__":
    main()
