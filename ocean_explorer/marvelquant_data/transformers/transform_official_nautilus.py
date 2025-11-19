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
from datetime import datetime, timedelta
import argparse
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
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
        date_int: Date as YYYYMMDD (e.g., 20240102)
        time_int: Time as seconds since midnight (e.g., 33300 = 09:15:00)
    
    Returns:
        datetime in UTC
    """
    # Parse date
    year = date_int // 10000
    month = (date_int % 10000) // 100
    day = date_int % 100
    
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
    start_date: str = None,
    end_date: str = None
) -> int:
    """
    Transform index data to Nautilus Bar format (OFFICIAL PATTERN).
    
    Args:
        input_dir: Directory containing raw parquet files
        catalog: Nautilus ParquetDataCatalog instance
        symbol: Symbol name (e.g., "NIFTY", "BANKNIFTY")
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    
    Returns:
        Number of bars created
    """
    logger.info(f"Transforming {symbol} index bars...")
    
    # Find all parquet files for this symbol
    symbol_dir = input_dir / "index" / symbol.lower()
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
    
    # Filter by date range only if dates are provided
    if start_date and end_date:
        start = pd.to_datetime(start_date) - pd.Timedelta(hours=6)  # Buffer for IST conversion
        end = pd.to_datetime(end_date) + pd.Timedelta(days=1)
        combined_df = combined_df[(combined_df['timestamp'] >= start) & 
                                   (combined_df['timestamp'] < end)]
        
        if combined_df.empty:
            logger.warning(f"No data in date range {start_date} to {end_date}")
            return 0
    
    # OFFICIAL PATTERN: Prepare DataFrame for BarDataWrangler
    # Required: columns ['open', 'high', 'low', 'close', 'volume'] with 'timestamp' as INDEX
    bar_df = combined_df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()

    # NOTE: Index data is already in rupees (not paise), NO conversion needed
    # Prices are correct as-is: 21476.00, not 214.76

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
    start_date: str = None,
    end_date: str = None,
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
    
    # Filter by date range only if dates are provided
    if start_date and end_date:
        start = pd.to_datetime(start_date) - pd.Timedelta(hours=6)
        end = pd.to_datetime(end_date) + pd.Timedelta(days=1)
        combined_df = combined_df[(combined_df['timestamp'] >= start) & 
                                   (combined_df['timestamp'] < end)]
        
        if combined_df.empty:
            return 0, pd.DataFrame()
    
    # Prepare for BarDataWrangler (OHLCV only, NO OI!)
    bar_df = combined_df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()

    # NOTE: Dated futures files (nifty_future_YYYYMMDD.parquet) are ALREADY in RUPEES
    # No conversion needed!

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
    prev_oi = 0
    for idx, row in combined_df.iterrows():
        current_oi = int(row["oi"])
        coi = current_oi - prev_oi
        prev_oi = current_oi
        ts_ns = int(row["timestamp"].timestamp() * 1_000_000_000)
        
        oi_data = FutureOI(
            instrument_id=instrument.id,
            oi=current_oi,
            coi=coi,
            ts_event=ts_ns,
            ts_init=ts_ns
        )
        oi_data_list.append(oi_data)
    
    # Write FutureOI to catalog (Arrow registered)
    if oi_data_list:
        oi_data_list.sort(key=lambda x: x.ts_init)
        catalog.write_data(oi_data_list)
        logger.info(f"✅ Saved {len(oi_data_list):,} FutureOI records")
    
    logger.info(f"✅ {symbol} futures: Created {len(bars):,} bars + {len(quote_ticks):,} QuoteTicks")
    return len(bars), None  # No longer returning DataFrame


def transform_options_bars(
    input_dir: Path,
    catalog: ParquetDataCatalog,
    symbol: str,
    start_date: str = None,
    end_date: str = None
) -> int:
    """
    Transform options data to Nautilus Bar format (OFFICIAL PATTERN).
    Note: Processes limited files to avoid memory issues.
    """
    logger.info(f"Transforming {symbol} options bars...")
    
    symbol_dir = input_dir / "option" / symbol.lower()
    if not symbol_dir.exists():
        logger.warning(f"No options directory found: {symbol_dir}")
        return 0
    
    # CRITICAL: Only use dated files (nifty_call/put_YYYYMMDD.parquet) which are in RUPEES
    # Similar to futures Bug #5 fix - dated files don't need paise conversion
    all_files = list(symbol_dir.rglob("*.parquet"))

    # Filter for dated call/put files (already in rupees)
    symbol_lower = symbol.lower()
    dated_call_files = [f for f in all_files if f.stem.startswith(f"{symbol_lower}_call_")]
    dated_put_files = [f for f in all_files if f.stem.startswith(f"{symbol_lower}_put_")]
    parquet_files = dated_call_files + dated_put_files

    if not parquet_files:
        logger.warning(f"No dated option files found in {symbol_dir}")
        return 0

    logger.info(f"Using {len(parquet_files)} dated option files (already in rupees)")

    total_bars = 0
    total_quote_ticks = 0

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
            
            # Filter by date only if dates are provided
            if start_date and end_date:
                start = pd.to_datetime(start_date) - pd.Timedelta(hours=6)
                end = pd.to_datetime(end_date) + pd.Timedelta(days=1)
                df = df[(df['timestamp'] >= start) & (df['timestamp'] < end)]
                
                if df.empty:
                    continue
            
            # Group by option symbol
            for option_symbol, group in df.groupby('symbol'):
                try:
                    bar_df = group[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()

                    # NOTE: Dated option files (nifty_call/put_YYYYMMDD.parquet) are ALREADY in RUPEES
                    # No paise conversion needed! (Bug #8 fix)

                    # Data quality fixes
                    bar_df['volume'] = bar_df['volume'].clip(lower=0)
                    bar_df['high'] = bar_df[['high', 'close', 'open']].max(axis=1)
                    bar_df['low'] = bar_df[['low', 'close', 'open']].min(axis=1)
                    
                    bar_df = bar_df.set_index('timestamp')
                    bar_df = bar_df.sort_index()
                    
                    # Create OptionContract (proper Nautilus instrument type)
                    try:
                        # Parse option symbol
                        parsed = parse_nse_option_symbol(option_symbol)
                        instrument = create_options_contract(
                            symbol=option_symbol,
                            underlying=parsed['underlying'],
                            strike=parsed['strike'],
                            expiry=parsed['expiry'],
                            option_kind=parsed['option_type'],
                            venue="NSE"
                        )
                    except Exception as parse_error:
                        logger.warning(f"Parse failed for {option_symbol}: {parse_error}, using Equity")
                        instrument_id = InstrumentId(symbol=Symbol(option_symbol), venue=Venue("NSE"))
                        instrument = Equity(
                            instrument_id=instrument_id,
                            raw_symbol=Symbol(option_symbol),
                            currency=Currency.from_str("INR"),
                            price_precision=2,
                            price_increment=Price(0.05, 2),
                            lot_size=Quantity.from_int(25 if "NIFTY" in option_symbol else 15),
                            ts_event=0,
                            ts_init=0,
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
                    if "oi" in group.columns:
                        oi_data_list = []
                        prev_oi = 0
                        for idx, oi_row in group.iterrows():
                            current_oi = int(oi_row["oi"])
                            coi = current_oi - prev_oi
                            prev_oi = current_oi
                            ts_ns = int(oi_row["timestamp"].timestamp() * 1_000_000_000)

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
                            catalog.write_data(oi_data_list)


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


    parser = argparse.ArgumentParser(
        description="Transform NSE data to Nautilus catalog (Official Pattern)"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("C:/csv_to_parquet/parquet_2011/2011/April/01"),
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
        help="Symbols to transform (omit to auto-discover from filenames)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD) - optional, processes all data if not provided"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD) - optional, processes all data if not provided"
    )
    
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("NAUTILUS DATA TRANSFORMATION - OFFICIAL PATTERN")
    logger.info("Following: nautilus_trader/examples/backtest/ patterns")
    logger.info("="*80)
    logger.info(f"Input: {args.input_dir}")
    logger.info(f"Output: {args.output_dir}")
    # Auto-discover symbols from filenames if not provided
    if args.symbols is None:
        discovered = sorted({p.stem.upper() for p in Path(args.input_dir).rglob("*.parquet")})
        args.symbols = discovered
        logger.info(f"Discovered symbols from filenames: {args.symbols}")
    else:
        logger.info(f"Symbols: {args.symbols}")
    if args.start_date and args.end_date:
        logger.info(f"Date range: {args.start_date} to {args.end_date}")
    else:
        logger.info("Processing all available data (no date filter)")
    logger.info("="*80)
    
    # Create catalog
    catalog = ParquetDataCatalog(path=str(args.output_dir))
    
    total_bars = 0
    
    # Transform index data
    for symbol in args.symbols:
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
    for symbol in args.symbols:
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
    for symbol in args.symbols:
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
    print("="*80)
    print("\nData structure:")
    print(f"  Bar data: {args.output_dir}/bar/")
    print(f"  OI data: {args.output_dir}/futures_oi/")
    print(f"  Instruments: {args.output_dir}/instrument/")
    print("\nNext steps:")
    print("  1. Verify data: catalog.bars()")
    print("  2. Run backtest with transformed data")
    print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description="Transform NSE data to Nautilus catalog (Official Pattern)"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("C:/csv_to_parquet/parquet_2011/2011/April/01"),
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
        help="Symbols to transform (omit to auto-discover from filenames)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD) - optional, processes all data if not provided"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD) - optional, processes all data if not provided"
    )
    
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("NAUTILUS DATA TRANSFORMATION - OFFICIAL PATTERN")
    logger.info("Following: nautilus_trader/examples/backtest/ patterns")
    logger.info("="*80)
    logger.info(f"Input: {args.input_dir}")
    logger.info(f"Output: {args.output_dir}")
    # Auto-discover symbols from filenames if not provided
    if args.symbols is None:
        discovered = sorted({p.stem.upper() for p in Path(args.input_dir).rglob("*.parquet")})
        args.symbols = discovered
        logger.info(f"Discovered symbols from filenames: {args.symbols}")
    else:
        logger.info(f"Symbols: {args.symbols}")
    if args.start_date and args.end_date:
        logger.info(f"Date range: {args.start_date} to {args.end_date}")
    else:
        logger.info("Processing all available data (no date filter)")
    logger.info("="*80)
    
    # Create catalog
    catalog = ParquetDataCatalog(path=str(args.output_dir))
    
    total_bars = 0
    
    # Transform index data
    for symbol in args.symbols:
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
    for symbol in args.symbols:
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
    for symbol in args.symbols:
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
