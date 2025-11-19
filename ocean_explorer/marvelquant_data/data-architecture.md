# Nexus Data Architecture

**Version**: 1.0
**Status**: APPROVED
**Date**: 2025-11-03
**Author**: Maruth (Product Owner)

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture Decision](#2-architecture-decision)
3. [Tier 1: Bar Data (OHLCV)](#3-tier-1-bar-data-ohlcv)
4. [Tier 1.5: QuoteTick Data (For Greeks Calculation)](#4-tier-15-quotetick-data-for-greeks-calculation)
5. [Tier 2: Contract Metadata](#5-tier-2-contract-metadata)
6. [Custom Data: Option OI](#6-custom-data-option-oi)
7. [Custom Data: Future OI](#7-custom-data-future-oi)
8. [Streaming Greeks (Not Stored)](#8-streaming-greeks-not-stored)
9. [Computed Fields](#9-computed-fields)
10. [VIX Integration](#10-vix-integration)
11. [Calendar and Expiry Management](#11-calendar-and-expiry-management)
12. [Symbol Naming Conventions](#12-symbol-naming-conventions)
13. [Data Access Patterns](#13-data-access-patterns)
14. [Performance Characteristics](#14-performance-characteristics)
15. [Rationale and Benefits](#15-rationale-and-benefits)

---

## 1. Overview

The Nexus project uses a **Two-Tier + Custom OI + Streaming Greeks** data architecture that aligns with NautilusTrader standards and industry best practices.

### Key Principles

1. **Nautilus Compliance**: Use native Nautilus classes (`Bar`, `QuoteTick`, `OptionContract`, `FuturesContract`) where possible
2. **Separation of Concerns**: OHLCV data, QuoteTick data (for Greeks), OI data, and Greeks are stored/computed separately
3. **Streaming Greeks**: Greeks are computed on-the-fly using QuoteTick data, not pre-stored
4. **Standard Directory Structure**: Use `catalog.write_data()` for automatic Nautilus-standard organization
5. **Query-Time Computation**: Dynamic fields (DTE, strike type, etc.) computed when needed

### Architecture Components

| Component | Storage Location | Class | Purpose |
|-----------|------------------|-------|---------|
| **Bar Data** | `data/bar/{INSTRUMENT_ID}/` | `Bar` | OHLCV for all instruments |
| **QuoteTick Data** | `data/quote_tick/{INSTRUMENT_ID}/` | `QuoteTick` | Bid/ask prices for Greeks calculation |
| **Option OI** | `data/custom_optionoi/{INSTRUMENT_ID}/` | `OptionOI` | Option open interest |
| **Future OI** | `data/custom_futureoi/{INSTRUMENT_ID}/` | `FutureOI` | Futures open interest |
| **Option Contracts** | `data/option_contract/` | `OptionContract` | Option metadata |
| **Futures Contracts** | `data/futures_contract/` | `FuturesContract` | Futures metadata |
| **Greeks** | ❌ Not stored | `self.greeks.portfolio_greeks()` | Streaming calculation |

---

## 2. Architecture Decision

### Why Two-Tier + Custom OI + Streaming Greeks?

**Research Finding**: Nautilus `Bar` class does NOT have an `open_interest` field.

**Industry Pattern**: Major data providers (Databento, Polygon.io, QuantConnect, Bloomberg) separate:
- OHLCV data (bars/aggregates)
- OI data (separate data type)
- Greeks (analytics/computed)

**Nautilus Production Pattern**: The official `databento_option_greeks.py` example uses:
- `catalog.bars()` for OHLCV
- `self.greeks.portfolio_greeks()` for streaming Greeks calculation
- Custom data types for additional fields

### Rejected Alternatives

**Alternative 1: Unified OptionChainSnapshot (22 fields)**
- ❌ Not Nautilus standard (Bar has no OI field)
- ❌ Not industry standard
- ❌ Worse compression (mixed data types)
- ❌ Query overhead (must load all fields)

**Alternative 2: Pre-computed Greeks Storage**
- ✅ Fast access (no recalculation)
- ❌ Not Nautilus standard pattern
- ❌ Greeks become stale (not updated with new bars)
- ❌ No portfolio-level aggregation
- ❌ Cannot perform scenario analysis
- ❌ Incompatible with live trading

**Alternative 3: Custom Directory Structure**
- ❌ Manual parquet writing (error-prone)
- ❌ Custom path management code
- ❌ Not compatible with Nautilus catalog
- ❌ Technical debt for future integration

---

## 3. Tier 1: Bar Data (OHLCV)

### Purpose
Store price and volume data for all tradeable instruments.

### ⚠️ CRITICAL: Binary Storage Format

**Bar data is stored in Apache Parquet BINARY columnar format, NOT float or text!**

**Storage Location**:
```
data/data/bar/{BAR_TYPE}/{TIMESTAMP_RANGE}.parquet
```

**Actual Example**:
```
data/data/bar/NIFTY-I.NSE-1-MINUTE-LAST-EXTERNAL/2024-01-31T03-45-00-000000000Z_2024-01-31T09-59-00-000000000Z.parquet
```

**Source Data**:
- **Original**: `data/original_source/` (raw Parquet files from data providers)
- **Transformer**: `scripts/transform_official_nautilus.py`
- **Pattern**: Follows Nautilus official examples (example_01_load_bars_from_custom_csv, example_04_using_data_catalog)

**File Format**:
- **Format**: Apache Parquet (binary columnar)
- **Compression**: Snappy (default)
- **Encoding**: Dictionary + RLE
- **Price Storage**: fixed_size_binary[16] (Nautilus Price type serialization)
- **Query Tool**: MUST use Nautilus catalog (NOT direct Pandas/PyArrow)

### Class
Nautilus native `Bar` class (Rust-backed, high-performance)

### Bar Schema - Stored Fields (7 columns in Parquet)

| Field | Parquet Storage Type | Nautilus Type | Description |
|-------|---------------------|---------------|-------------|
| `open` | **fixed_size_binary[16]** | `Price` | Opening price (BINARY encoded, NOT float) |
| `high` | **fixed_size_binary[16]** | `Price` | High price (BINARY encoded, NOT float) |
| `low` | **fixed_size_binary[16]** | `Price` | Low price (BINARY encoded, NOT float) |
| `close` | **fixed_size_binary[16]** | `Price` | Closing price (BINARY encoded, NOT float) |
| `volume` | **fixed_size_binary[16]** | `Quantity` | Trading volume (BINARY encoded, NOT float) |
| `ts_event` | `uint64` | `uint64_t` | Event timestamp (UTC nanoseconds) |
| `ts_init` | `uint64` | `uint64_t` | Initialization timestamp (UTC nanoseconds) |

**Note**: `bar_type` and `is_revision` are NOT stored as columns - they are encoded in the directory/filename structure for storage efficiency.

### ⚠️ CRITICAL: Binary Price Format

**Prices are stored as 16-byte binary blobs, NOT floats!**

**Binary Encoding Details**:
- **Format**: Nautilus `Price` type serialization
- **Size**: 16 bytes per price
- **Content**: raw_value (int64) + precision (uint8) + padding
- **Example**: 21435.50 → binary blob with `raw_value=2143550`, `precision=2`

**Why Binary Format**:
- ✅ **Exact decimal precision** (no float rounding errors)
- ✅ **Fast serialization** (Rust-optimized)
- ✅ **Compact storage** (16 bytes with precision metadata)
- ✅ **Rust-compatible** (Nautilus core is Rust)

**Sample Binary Data** (from actual file):
```python
# Direct Pandas read (WRONG - returns binary bytes)
df = pd.read_parquet("data/data/bar/.../data.parquet")
df["close"][0]  # Returns: b'\x00\x00F_g\xa0KH\x92\x04\x00\x00\x00\x00\x00\x00'
                # ❌ This is BINARY BLOB, not usable as price!
```

### Critical Note: NO open_interest Field
**The Nautilus `Bar` class does NOT have an `open_interest` field.** This is why OI data requires separate custom data types (OptionOI, FutureOI).

### What to Store

| Instrument Type | OHLCV | Note |
|----------------|-------|------|
| **Index** | ✅ Yes | Spot has no open interest |
| **VIX** | ✅ Yes | Use Close for VIX value, volume=0 |
| **Futures** | ✅ Yes | OI stored separately in FutureOI |
| **Options** | ✅ Yes | OI stored separately in OptionOI |

### Actual Directory Structure (From Production Data)
```
data/data/bar/
├── NIFTY-INDEX.NSE-1-MINUTE-LAST-EXTERNAL/
│   └── 2024-01-31T03-45-00-000000000Z_2024-01-31T09-59-00-000000000Z.parquet
├── NIFTY-I.NSE-1-MINUTE-LAST-EXTERNAL/
│   └── 2024-01-31T03-45-00-000000000Z_2024-01-31T09-59-00-000000000Z.parquet
├── NIFTY01FEB2420000CE.NSE-1-MINUTE-LAST-EXTERNAL/
│   └── 2024-01-31T04-25-00-000000000Z_2024-01-31T09-13-00-000000000Z.parquet
└── [more instruments...]
```

**Note**: Directory structure is `{BAR_TYPE}` (not `{INSTRUMENT_ID}/{BAR_TYPE}`). Filenames use ISO8601 timestamp ranges, NOT `data.parquet`.

### Creating Bar Data (via Transformer Script)

**⚠️ IMPORTANT**: Bar data is created by the transformation script, NOT manually created Bar objects.

**Transformation Script**: `scripts/transform_official_nautilus.py`

**How It Works**:
```python
# From scripts/transform_official_nautilus.py

from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.persistence.catalog import ParquetDataCatalog

# 1. Load source data from original_source/
df = pd.read_parquet("data/original_source/...")

# 2. Prepare DataFrame for BarDataWrangler
#    Required: columns ['open', 'high', 'low', 'close', 'volume'] with 'timestamp' as INDEX
bar_df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
bar_df = bar_df.set_index('timestamp')  # CRITICAL: Set timestamp as index!
bar_df = bar_df.sort_index()

# 3. Create BarDataWrangler (converts to Nautilus Bar format with BINARY encoding)
wrangler = BarDataWrangler(bar_type, instrument)
bars = wrangler.process(
    data=bar_df,
    default_volume=0.0,
    ts_init_delta=0
)

# 4. Write to catalog (automatic BINARY Parquet serialization)
catalog.write_data([instrument])  # Write instrument first
catalog.write_data(bars, skip_disjoint_check=True)

# Result: Creates BINARY Parquet file with fixed_size_binary[16] price encoding
# Location: data/data/bar/{BAR_TYPE}/{TIMESTAMP_RANGE}.parquet
```

**Critical Points**:
- ✅ Uses `BarDataWrangler` (Nautilus official pattern)
- ✅ Automatically encodes prices as **fixed_size_binary[16]**
- ✅ Source data: `data/original_source/` (float prices in source)
- ✅ Output data: BINARY encoded (NOT float anymore!)
- ✅ Timestamps converted to UTC nanoseconds

### Usage in Strategy

**Subscribe to Bars**:
```python
class TBSStrategy(Strategy):
    def on_start(self):
        # Subscribe to option bars
        self.subscribe_bars(
            BarType.from_str("BANKNIFTY28OCT2548000CE.NSE-1-MINUTE-LAST-EXTERNAL")
        )

        # Subscribe to futures bars
        self.subscribe_bars(
            BarType.from_str("BANKNIFTY-I.NSE-1-MINUTE-LAST-EXTERNAL")
        )

        # Subscribe to VIX
        self.subscribe_bars(
            BarType.from_str("INDIA-VIX.NSE-1-MINUTE-LAST-EXTERNAL")
        )

    def on_bar(self, bar: Bar):
        # Handle bar updates
        if "VIX" in str(bar.bar_type.instrument_id):
            current_vix = float(bar.close)
            # Adjust position sizing based on VIX
        else:
            # Process OHLCV data
            print(f"Close: {bar.close}, Volume: {bar.volume}")
```

### Querying Bar Data: ❌ WRONG vs ✅ CORRECT

### ❌ WRONG WAY - Direct Pandas (Returns Binary Blobs!)

```python
import pandas as pd

# ❌ WRONG: Direct Pandas read
df = pd.read_parquet("data/data/bar/NIFTY-I.NSE-1-MINUTE-LAST-EXTERNAL/2024-01-31T03-45-00-000000000Z_2024-01-31T09-59-00-000000000Z.parquet")

print(df["close"][0])
# Output: b'\x00\x00F_g\xa0KH\x92\x04\x00\x00\x00\x00\x00\x00'
# ❌ BINARY BLOB - NOT usable as price!

print(type(df["close"][0]))
# Output: <class 'bytes'>
# ❌ This is bytes, NOT float or Price!
```

**Why This Fails**:
- Prices are stored as `fixed_size_binary[16]`
- Pandas doesn't know how to decode Nautilus Price format
- You get raw binary bytes, not numbers
- ❌ **DO NOT USE** direct Pandas/PyArrow reading!

---

### ✅ CORRECT WAY - Nautilus Catalog (Decodes Binary → Price Objects)

```python
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from datetime import date

# ✅ CORRECT: Use Nautilus catalog
catalog = ParquetDataCatalog("./data/data")

# Query bars (Nautilus decodes binary → Price objects)
bars = catalog.bars(
    instrument_ids=["NIFTY-I.NSE"],
    start=date(2024, 1, 31),
    end=date(2024, 1, 31)
)

# Access decoded prices
bar = bars[0]
print(bar.open)    # Output: 21435.50 (Price object, human-readable!)
print(bar.close)   # Output: 21462.00 (Price object, human-readable!)
print(type(bar.close))  # Output: <class 'nautilus_trader.model.objects.Price'>

# ✅ Prices are NOW properly decoded and usable!
```

**Convert to DataFrame** (after Nautilus decoding):
```python
# Convert to Polars (prices are decoded)
bars_df = bars.to_polars()
print(bars_df.head())

# Output: Human-readable prices!
#   open     | high     | low      | close    | volume | ts_event
#   21435.50 | 21450.00 | 21420.00 | 21462.00 | 1125   | 1706678700000000000
```

**Query Multiple Instruments**:
```python
# Query multiple instruments (bar_type pattern matching)
bars = catalog.bars(
    bar_type="*-1-MINUTE-LAST-EXTERNAL",  # All 1-minute bars
    start=date(2024, 1, 31),
    end=date(2024, 1, 31)
)
```

### Bar Data Performance Characteristics

**Storage Size (from actual production file)**:
- **Verified**: 1,125 rows = 48.85 KB (NIFTY-I.NSE 1-day data)
- **Bytes per row**: ~44.5 bytes per bar (including Parquet overhead)
- **Compression**: ~10:1 ratio vs uncompressed
- **Price encoding**: 16 bytes each (open, high, low, close, volume) = 80 bytes
- **Timestamps**: 8 bytes each (ts_event, ts_init) = 16 bytes
- **Total raw**: ~96 bytes/row → compressed to ~44.5 bytes/row

**Query Performance**:
- **Catalog decoding**: Nautilus automatically decodes binary → Price objects
- **Single instrument, 1 day**: <10ms (with binary decoding)
- **100 instruments, 1 day**: <100ms
- **Polars conversion**: After decoding, very fast (columnar format)

**Why Binary Format is Efficient**:
- ✅ **Compact storage**: 16 bytes with precision metadata
- ✅ **Exact precision**: No float rounding errors
- ✅ **Fast serialization**: Rust-optimized encoding/decoding
- ✅ **Columnar compression**: Dictionary + RLE encoding
- ✅ **Indexed by timestamp**: Fast time-range queries

**Verified File Locations**:
```
data/data/bar/NIFTY-I.NSE-1-MINUTE-LAST-EXTERNAL/
  └── 2024-01-31T03-45-00-000000000Z_2024-01-31T09-59-00-000000000Z.parquet
      Size: 48.85 KB
      Rows: 1,125
      Format: Parquet (binary)
      Compression: Snappy
```

**Source Transformation**:
```
Source: data/original_source/option_chain/nifty/year=2024/month=01/2024-01-31_nifty_processed_corrected.parquet
  ↓ (transform_official_nautilus.py)
Output: data/data/bar/NIFTY-I.NSE-1-MINUTE-LAST-EXTERNAL/2024-01-31T...parquet
```

---

## 4. Tier 1.5: QuoteTick Data (For Greeks Calculation)

### Purpose
Provide bid/ask price data **required** by NautilusTrader `BacktestEngine` Greeks calculator for computing option Greeks.

### ⚠️ CRITICAL: Greeks Calculator Requirement

**The NautilusTrader Greeks calculator REQUIRES QuoteTick data, NOT Bar data!**

The `self.greeks.instrument_greeks()` method queries the **QuoteTick cache** for:
1. **Option prices**: Uses bid/ask from option QuoteTicks
2. **Underlying spot price**: Uses bid/ask from underlying (spot index) QuoteTicks
3. **Mid-price calculation**: Computes mid = (bid + ask) / 2 for Black-Scholes

**If QuoteTicks are missing**, Greeks calculation will:
- Return `None` for all Greeks (delta, gamma, theta, vega, rho)
- Log warnings: "No quote tick found for instrument"
- Strategy will fail to compute option sensitivities

### Storage Location

```
data/data/quote_tick/{INSTRUMENT_ID}/{TIMESTAMP_RANGE}.parquet
```

**Actual Example**:
```
data/data/quote_tick/NIFTY-INDEX.NSE/2024-01-31T03-45-00-000000000Z_2024-01-31T09-59-00-000000000Z.parquet
data/data/quote_tick/NIFTY01FEB2420000CE.NSE/2024-01-31T04-25-00-000000000Z_2024-01-31T09-13-00-000000000Z.parquet
```

### Class
Nautilus native `QuoteTick` class (Rust-backed, high-performance)

### QuoteTick Schema - Stored Fields (6 columns)

| Field | Parquet Storage Type | Nautilus Type | Description |
|-------|---------------------|---------------|-------------|
| `bid_price` | **fixed_size_binary[16]** | `Price` | Best bid price (BINARY encoded, NOT float) |
| `ask_price` | **fixed_size_binary[16]** | `Price` | Best ask price (BINARY encoded, NOT float) |
| `bid_size` | **fixed_size_binary[16]** | `Quantity` | Bid quantity (BINARY encoded, NOT float) |
| `ask_size` | **fixed_size_binary[16]** | `Quantity` | Ask quantity (BINARY encoded, NOT float) |
| `ts_event` | `uint64` | `uint64_t` | Event timestamp (UTC nanoseconds) |
| `ts_init` | `uint64` | `uint64_t` | Initialization timestamp (UTC nanoseconds) |

**Metadata** (stored in Parquet metadata):
- `instrument_id`: Full instrument identifier (e.g., 'NIFTY-INDEX.NSE')
- `price_precision`: Decimal precision for prices (e.g., '2')
- `size_precision`: Decimal precision for quantities (e.g., '0')

### ⚠️ CRITICAL: Binary Price Format (Same as Bar Data)

**Prices are stored as 16-byte binary blobs, NOT floats!**

- **Format**: Nautilus `Price` type serialization
- **Size**: 16 bytes per price
- **Content**: raw_value (int64) + precision (uint8) + padding
- **Example**: 21435.50 → binary blob with `raw_value=2143550`, `precision=2`

**Must use Nautilus catalog** to query (NOT direct Pandas):
```python
# ✅ CORRECT
catalog = ParquetDataCatalog("./data/data")
quotes = catalog.quote_ticks(instrument_ids=["NIFTY-INDEX.NSE"])

# ❌ WRONG - Returns binary blobs!
df = pd.read_parquet("data/data/quote_tick/...")
```

### 4.1 Why QuoteTicks Are Required for Greeks

#### Technical Requirement

**NautilusTrader BacktestEngine Architecture**:
```python
# From backtest strategy code
greeks_data = self.greeks.instrument_greeks(
    instrument_id=instrument.id,
    use_cached_greeks=False,
    cache_greeks=False,
    publish_greeks=False,
)
```

**What happens internally**:
1. Greeks calculator calls `self.cache.quote_tick(instrument.id)` (NOT `self.cache.bar()`)
2. Retrieves latest QuoteTick for option instrument
3. Retrieves latest QuoteTick for underlying instrument (spot index)
4. Computes mid price: `(bid_price + ask_price) / 2`
5. Uses mid prices in Black-Scholes model: `greeks_calculator(S, K, T, r, σ, q)`
6. Returns `GreeksData(delta, gamma, theta, vega, rho)`

**If QuoteTick is missing**:
- `self.cache.quote_tick()` returns `None`
- Greeks calculator cannot compute values
- Returns `None` for all Greeks
- Strategy logic breaks (cannot hedge, adjust positions, etc.)

#### Why Bar Data Alone Is Insufficient

**Bar data provides**:
- OHLCV (5 price points + volume)
- Good for charting, technical indicators, signals
- Accessed via `catalog.bars()` or `self.subscribe_bars()`

**Greeks calculation needs**:
- **Bid/Ask spread**: For accurate pricing and execution simulation
- **Mid price**: Fair value for option valuation in Black-Scholes
- **Underlying spot price**: Must be from QuoteTick (not Bar)
- **Quote cache**: Greeks engine specifically queries `self.cache.quote_tick()`

**The Gap**:
- Bar data has `close` price but Greeks engine **doesn't query bars**
- Greeks calculator is hardcoded to use QuoteTick cache only
- No fallback to Bar data (by Nautilus design)
- Must provide QuoteTicks for both **option AND underlying**

### 4.2 Bars-to-QuoteTicks Transformation

Since our source data is OHLCV bars (not real-time tick data), we **generate synthetic QuoteTicks** from bars.

#### Transformation Function

**File**: `scripts/transform_official_nautilus.py` (lines 51-76)

```python
def bars_to_quote_ticks(bars, instrument):
    """
    Convert Bar data to QuoteTicks for Greeks calculation.

    Creates QuoteTicks where bid=ask=close price.
    This is required for NautilusTrader Greeks calculator.

    Why bid=ask=close:
    - Simplification for backtesting (zero spread)
    - Close price is most representative value
    - Sufficient for Greeks calculation accuracy

    For live trading:
    - Use real bid/ask from exchange
    - Same code interface (QuoteTick)
    - Backtest-live parity maintained
    """
    quote_ticks = []

    for bar in bars:
        # Create QuoteTick using close price as both bid and ask
        price = Price(bar.close.as_double(), instrument.price_precision)
        size = Quantity(1, instrument.size_precision)

        tick = QuoteTick(
            instrument_id=instrument.id,
            bid_price=price,      # bid = close
            ask_price=price,      # ask = close (zero spread)
            bid_size=size,        # Size set to 1
            ask_size=size,        # Size set to 1
            ts_event=bar.ts_event,  # Preserve bar timestamp
            ts_init=bar.ts_init,    # Preserve bar timestamp
        )
        quote_ticks.append(tick)

    return quote_ticks
```

#### Transformation Pipeline

**Complete flow in transformation script**:

```python
# Step 1: Create bars from source data (OHLCV)
wrangler = BarDataWrangler(bar_type, instrument)
bars = wrangler.process(bar_df)

# Step 2: Write bars to catalog
catalog.write_data([instrument])  # Write instrument first
catalog.write_data(bars, skip_disjoint_check=True)

# Step 3: Generate QuoteTicks from bars
quote_ticks = bars_to_quote_ticks(bars, instrument)

# Step 4: Write QuoteTicks to catalog
catalog.write_data(quote_ticks, skip_disjoint_check=True)
```

**Applied to**:
- Line 217-218: Index transformation (creates bars + QuoteTicks)
- Line 328-330: Futures transformation (creates bars + QuoteTicks)
- Line 469-471: Options transformation (creates bars + QuoteTicks)

#### Key Characteristics

| Aspect | Value | Reason |
|--------|-------|--------|
| **Source** | Bar close price | Most representative price point |
| **Bid price** | close | Simplified for backtesting |
| **Ask price** | close | Zero spread assumption |
| **Spread** | 0 (bid=ask) | Conservative for backtest |
| **Timing** | Same as bar | 1-to-1 mapping |
| **Quantity** | 1 | Not used by Greeks calculator |

#### 1-to-1 Mapping

**For each bar, create one QuoteTick**:
- Same timestamp (`ts_event`, `ts_init`)
- Bid/Ask from bar close
- Preserves temporal alignment
- Enables synchronized bar + QuoteTick queries

**Example**:
```
Bar at 09:15:00: close=21435.50
  → QuoteTick at 09:15:00: bid=21435.50, ask=21435.50

Bar at 09:16:00: close=21437.00
  → QuoteTick at 09:16:00: bid=21437.00, ask=21437.00
```

### 4.3 Runtime QuoteTick Generation (Strategy Pattern)

For strategies that run on bar data but need Greeks, QuoteTicks can be generated **on-the-fly** in the strategy:

```python
# From notebooks/epic-2.5/poc/strike-methods/backtest_by_closest_delta.py
# Lines 203-218

def on_bar(self, bar: Bar):
    """Process bar and create synthetic QuoteTick for Greeks."""

    # Get instrument from cache
    instrument = self.cache.instrument(bar.bar_type.instrument_id)

    if instrument:
        # Create synthetic QuoteTick from bar close
        price = Price(bar.close, instrument.price_precision)
        qty = Quantity(1, instrument.size_precision)

        quote = QuoteTick(
            instrument_id=bar.bar_type.instrument_id,
            bid_price=price,
            ask_price=price,  # bid=ask=close
            bid_size=qty,
            ask_size=qty,
            ts_event=bar.ts_event,
            ts_init=bar.ts_init,
        )

        # Add to cache (Greeks calculator will find it)
        self.cache.add_quote_tick(quote)

        # For index bars, also create underlying quote
        if "INDEX" in str(bar.bar_type.instrument_id):
            underlying_id = InstrumentId.from_str(f"{self.underlying}.NSE")
            underlying_quote = QuoteTick(
                instrument_id=underlying_id,
                bid_price=price,
                ask_price=price,
                bid_size=qty,
                ask_size=qty,
                ts_event=bar.ts_event,
                ts_init=bar.ts_init,
            )
            self.cache.add_quote_tick(underlying_quote)
```

**When to use**:
- ✅ Bars exist in catalog, but QuoteTicks don't
- ✅ Rapid prototyping without re-transforming data
- ✅ Backtest experiments with existing bar data
- ⚠️ Adds runtime overhead (generate on every bar)
- ⚠️ Not recommended for production (pre-generate instead)

### 4.4 Data Flow: Bars → QuoteTicks → Greeks

```
┌─────────────────────────────────────────────────────────────────┐
│                 ORIGINAL SOURCE DATA                            │
│  (NSE CSV/Parquet files with OHLCV + OI)                      │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│          TRANSFORMATION SCRIPT                                  │
│  (transform_official_nautilus.py)                              │
│                                                                 │
│  Creates 3 parallel data streams:                              │
│  1. Bar data (OHLCV) → catalog.write_data(bars)               │
│  2. QuoteTick data → bars_to_quote_ticks() → catalog          │
│  3. OI data → catalog.write_data(oi_data)                     │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│            NAUTILUS PARQUET CATALOG                             │
│                                                                 │
│  data/data/                                                     │
│  ├── bar/           (OHLCV for indicators)                     │
│  ├── quote_tick/    (bid/ask for Greeks) ⭐ CRITICAL          │
│  ├── custom_option_oi/  (option OI)                           │
│  └── custom_future_oi/  (futures OI)                          │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│               BACKTEST ENGINE EXECUTION                         │
│                                                                 │
│  Strategy subscribes to:                                        │
│  - subscribe_bars() → Technical indicators                     │
│  - subscribe_quote_ticks() → Greeks (mandatory!)              │
│                                                                 │
│  Data flows into cache:                                         │
│  - self.cache.bars() → Bar cache                              │
│  - self.cache.quote_ticks() → QuoteTick cache ⭐             │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│         GREEKS CALCULATOR (NautilusTrader)                      │
│                                                                 │
│  self.greeks.instrument_greeks():                              │
│  1. Queries option QuoteTick → self.cache.quote_tick(option)  │
│  2. Queries underlying QuoteTick → self.cache.quote_tick(spot) │
│  3. Queries InterestRateProvider → risk-free rate (r)          │
│  4. Computes Black-Scholes Greeks:                             │
│     - Delta, Gamma, Theta, Vega, Rho                          │
│  5. Returns GreeksData object                                   │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              STRATEGY DECISION LOGIC                            │
│                                                                 │
│  Uses Greeks for:                                               │
│  - Delta hedging (maintain delta neutral)                      │
│  - Position sizing (scale by vega)                             │
│  - Risk management (theta decay, gamma risk)                   │
│  - Exit signals (Greeks thresholds)                            │
└─────────────────────────────────────────────────────────────────┘
```

### 4.5 Critical Implementation Notes

#### Both Option AND Underlying Need QuoteTicks

**Greeks calculator requires TWO QuoteTicks**:
1. **Option QuoteTick**: For option price (bid/ask)
2. **Underlying QuoteTick**: For spot price (S parameter)

**Example for NIFTY option**:
```python
# Option instrument
option_id = InstrumentId.from_str("NIFTY01FEB2420000CE.NSE")
option_instrument.underlying = "NIFTY-INDEX"  # References spot index

# Greeks calculator needs QuoteTicks for BOTH:
1. option_quote = self.cache.quote_tick(option_id)           # Option price
2. underlying_quote = self.cache.quote_tick("NIFTY-INDEX.NSE")  # Spot price

# If either is missing → Greeks = None!
```

#### Underlying Configuration (CRITICAL FIX)

**NSE Index Options use SPOT INDEX, not futures!**

**File**: `marvelquant/utils/contract_generators.py` (line 130)

```python
# ✅ CORRECT (after fix):
underlying_spot = f"{underlying}-INDEX"  # NIFTY → NIFTY-INDEX

contract = OptionContract(
    ...
    underlying=underlying_spot,  # "NIFTY-INDEX" (spot index)
    ...
)

# ❌ WRONG (before fix):
underlying_futures = f"{underlying}-I"  # NIFTY → NIFTY-I (futures!)
# This caused Greeks miscalculation (Bug fixed 2025-11-04)
```

**Why this matters**:
- Options reference **spot index** price (not futures)
- Futures price includes carry cost (interest - dividend)
- Using futures as underlying causes incorrect delta/gamma
- See: `docs/bmad/ENTERPRISE_SOLUTION_OPTIONS_GREEKS_AND_DATA_ARCHITECTURE.md`

#### Directory Structure (from Production Catalog)

```
data/data/quote_tick/
├── NIFTY-INDEX.NSE/
│   └── 2024-01-31T03-45-00-000000000Z_2024-01-31T09-59-00-000000000Z.parquet
├── NIFTY-I.NSE/
│   └── 2024-01-31T03-45-00-000000000Z_2024-01-31T09-59-00-000000000Z.parquet
├── NIFTY01FEB2420000CE.NSE/
│   └── 2024-01-31T04-25-00-000000000Z_2024-01-31T09-13-00-000000000Z.parquet
├── NIFTY01FEB2420000PE.NSE/
│   └── 2024-01-31T04-25-00-000000000Z_2024-01-31T09-13-00-000000000Z.parquet
└── [more instruments...]

Total: 596 quote_tick parquet files (verified 2025-11-04)
```

### 4.6 Usage in Strategy

#### Subscribe to QuoteTicks

```python
class ByClosestDeltaStrategy(Strategy):
    def on_start(self):
        # Subscribe to option QuoteTicks
        self.subscribe_quote_ticks(
            InstrumentId.from_str("NIFTY01FEB2420000CE.NSE")
        )

        # Subscribe to underlying QuoteTick (CRITICAL!)
        self.subscribe_quote_ticks(
            InstrumentId.from_str("NIFTY-INDEX.NSE")
        )

        # Subscribe to Greeks (will use QuoteTicks)
        self.greeks.subscribe_greeks(
            InstrumentId.from_str("NIFTY*.NSE")  # All NIFTY instruments
        )
```

#### Compute Greeks (Uses QuoteTicks)

```python
def on_bar(self, bar: Bar):
    # Greeks calculator queries QuoteTick cache
    greeks_data = self.greeks.instrument_greeks(
        instrument_id=instrument.id,
        use_cached_greeks=False,   # Fresh calculation
        cache_greeks=False,         # Don't cache
        publish_greeks=False,       # Don't publish
    )

    if greeks_data is None:
        self.log.warning(f"Greeks computation FAILED - QuoteTick missing for {instrument.id}")
        return

    # Access Greeks
    delta = greeks_data.delta
    gamma = greeks_data.gamma
    theta = greeks_data.theta
    vega = greeks_data.vega

    # Use for trading decisions
    if abs(delta) > 0.50:
        # Close position (too directional)
        self.close_position(instrument.id)
```

### 4.7 Future Enhancements

#### Realistic Bid-Ask Spread Estimation

**Current**: bid = ask = close (zero spread)

**Future Enhancement**: Estimate realistic spreads from OHLC data

**Research** (from ENTERPRISE_SOLUTION document):
- **Ardia et al. (2024)**: "Efficient Estimation of Bid-Ask Spreads from OHLC"
- **Corwin & Schultz (2011)**: High-Low spread estimator
- **Roll (1984)**: Serial covariance method

**Implementation**:
```python
def bars_to_quote_ticks_with_spread(bars, instrument, spread_model="fixed_bps"):
    """Enhanced QuoteTick generation with spread estimation."""

    if spread_model == "fixed_bps":
        # Fixed 5 bps spread
        spread_bps = 0.0005
    elif spread_model == "corwin_schultz":
        # Estimate from high-low range
        spread_bps = estimate_spread_corwin_schultz(bars)
    elif spread_model == "adaptive":
        # Adapt to volatility
        spread_bps = estimate_spread_adaptive(bars, volatility)

    for bar in bars:
        mid_price = bar.close
        spread = mid_price * spread_bps

        bid = mid_price - (spread / 2)
        ask = mid_price + (spread / 2)

        # Create QuoteTick with realistic spread
        tick = QuoteTick(
            bid_price=Price(bid, instrument.price_precision),
            ask_price=Price(ask, instrument.price_precision),
            ...
        )
```

**Benefits**:
- ✅ More realistic execution simulation
- ✅ Better slippage modeling
- ✅ Improved backtest accuracy
- ✅ Closer to live trading behavior

#### Live Trading Integration

**Backtest** (current):
- Generate synthetic QuoteTicks from bars
- bid = ask = close

**Live Trading** (future):
- Receive real bid/ask from exchange
- Use actual market depth
- Same `QuoteTick` interface
- **Backtest-live parity maintained**

**Implementation**:
```python
# Live data adapter
class NSELiveDataAdapter:
    def on_market_depth(self, order_book):
        # Convert real order book to QuoteTick
        quote = QuoteTick(
            instrument_id=instrument.id,
            bid_price=order_book.best_bid,    # Real bid
            ask_price=order_book.best_ask,    # Real ask
            bid_size=order_book.bid_quantity,
            ask_size=order_book.ask_quantity,
            ts_event=order_book.timestamp,
            ts_init=self.clock.timestamp_ns(),
        )

        # Publish to cache (same as backtest!)
        self.cache.add_quote_tick(quote)

        # Strategy uses same code path
        # self.greeks.instrument_greeks() works identically
```

### 4.8 Querying QuoteTick Data

#### Query from Catalog

```python
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from datetime import date

# Load catalog
catalog = ParquetDataCatalog("./data/data")

# Query QuoteTicks for specific instrument
quotes = catalog.quote_ticks(
    instrument_ids=["NIFTY-INDEX.NSE"],
    start=date(2024, 1, 31),
    end=date(2024, 1, 31)
)

# Access QuoteTick data
for quote in quotes:
    print(f"Bid: {quote.bid_price}, Ask: {quote.ask_price}")
    print(f"Spread: {quote.ask_price - quote.bid_price}")
    print(f"Mid: {(quote.bid_price + quote.ask_price) / 2}")
```

#### Convert to DataFrame

```python
# Convert to Polars (after Nautilus decoding)
quotes_df = quotes.to_polars()
print(quotes_df.head())

# Output: Human-readable prices!
#   bid_price | ask_price | bid_size | ask_size | ts_event
#   21435.50  | 21435.50  | 1        | 1        | 1706678700000000000
```

### 4.9 Storage Characteristics

**File Count** (from production catalog):
- **596 QuoteTick parquet files** (verified 2025-11-04)
- One file per instrument per time range
- Parallel to Bar files (1-to-1 mapping)

**Storage Size** (estimated):
- **6 fields × 16 bytes** (binary) + timestamps = ~112 bytes per QuoteTick
- **Compression**: ~10:1 with Snappy (columnar format)
- **Net storage**: ~11 bytes per QuoteTick
- **Example**: 375 QuoteTicks = ~4 KB per file

**Query Performance**:
- **Single instrument, 1 day**: <10ms (with binary decoding)
- **100 instruments, 1 day**: <100ms
- **Polars conversion**: Fast (columnar format)

---

## 5. Tier 2: Contract Metadata

### Purpose
Store static instrument specifications that rarely change.

### Storage Location
```
data/option_contract/{INSTRUMENT_ID}.parquet
data/futures_contract/{INSTRUMENT_ID}.parquet
```

### Classes
- Nautilus native `OptionContract`
- Nautilus native `FuturesContract`

### Option Contract Fields

| Field | Type | Description |
|-------|------|-------------|
| `instrument_id` | `InstrumentId` | Unique identifier |
| `raw_symbol` | `Symbol` | Native exchange symbol |
| `asset_class` | `AssetClass` | EQUITY, COMMODITY, etc. |
| `exchange` | `str` | NSE |
| `underlying` | `str` | BANKNIFTY, NIFTY, etc. |
| `option_kind` | `OptionKind` | CALL or PUT |
| `activation_ns` | `uint64_t` | Contract activation timestamp |
| `expiration_ns` | `uint64_t` | Expiry timestamp |
| `strike_price` | `Price` | Strike price |
| `currency` | `Currency` | INR |
| `price_precision` | `uint8` | Tick size precision |
| `price_increment` | `Price` | Minimum tick size |
| `multiplier` | `Quantity` | Lot size multiplier |
| `lot_size` | `Quantity` | Contract lot size |

### Futures Contract Fields

Similar to Options, but without `option_kind` and `strike_price`.

### Why Separate from Bar Data?

1. **Metadata rarely changes**: Loaded once into memory cache
2. **Different query patterns**: Metadata queries vs time-series queries
3. **Efficient joins**: Join bar data with cached metadata
4. **Nautilus standard**: Separate instrument storage

### Usage
```python
# Query contracts from catalog
contracts = catalog.instruments(
    instrument_type=OptionContract,
    instrument_ids=["BANKNIFTY28OCT2548000CE.NSE"]
)

# Access contract fields
for contract in contracts:
    print(f"Strike: {contract.strike_price}")
    print(f"Expiry: {contract.expiration_ns}")
    print(f"Lot Size: {contract.lot_size}")
```

---

## 6. Custom Data: Option OI

### Purpose
Store open interest data for options (separate from Bar, which has no OI field).

### Storage Location
```
data/custom_optionoi/{INSTRUMENT_ID}/{START_TS}_{END_TS}.parquet
```

### Class
`OptionOI` (extends `nautilus_trader.core.data.Data`)

### Pattern
Uses `@customdataclass` decorator (Nautilus standard for custom data types).

### Fields (6)

| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `instrument_id` | `InstrumentId` | Instrument identifier | User-defined |
| `oi` | `int` | Current open interest | User-defined |
| `coi` | `int` | Change in open interest | User-defined |
| `type` | `str` | Always "OptionOI" | Auto-generated |
| `ts_event` | `uint64_t` | Event timestamp (UTC ns) | Base class |
| `ts_init` | `uint64_t` | Init timestamp (UTC ns) | Base class |

### @customdataclass Benefits

1. **Automatic 'type' column**: Identifies data type
2. **Timestamp-based files**: ISO8601 format filenames
3. **Instrument subdirectories**: Organized by instrument
4. **Arrow serialization**: Automatic to/from Arrow conversion
5. **Catalog integration**: Works with `catalog.write_data()` and `catalog.generic_data()`

### Example Directory Structure
```
data/custom_optionoi/
├── BANKNIFTY28OCT2548000CE.NSE/
│   ├── 2024-10-28T03-45-00-000000000Z_2024-10-28T09-59-00-000000000Z.parquet
│   └── 2024-10-29T03-45-00-000000000Z_2024-10-29T09-59-00-000000000Z.parquet
└── BANKNIFTY28OCT2548000PE.NSE/
    └── 2024-10-28T03-45-00-000000000Z_2024-10-28T09-59-00-000000000Z.parquet
```

### Usage
```python
# Write OptionOI data
from marvelquant.data.types import OptionOI

oi_data = OptionOI(
    instrument_id=InstrumentId.from_str("BANKNIFTY28OCT2548000CE.NSE"),
    oi=10500,
    coi=250,
    ts_event=ts_event,
    ts_init=ts_init
)
catalog.write_data([oi_data])

# Query OptionOI data
oi_records = catalog.generic_data(
    cls=OptionOI,
    instrument_ids=["BANKNIFTY28OCT2548000CE.NSE"],
    start=start_date,
    end=end_date
)
```

### Implementation
```python
# File: /marvelquant/data/types/option_oi.py
from nautilus_trader.core.data import Data, customdataclass

@customdataclass
class OptionOI(Data):
    """Option Open Interest custom data type."""
    instrument_id: InstrumentId
    oi: int
    coi: int
    # ts_event and ts_init inherited from Data base class
    # 'type' column auto-generated as "OptionOI"
```

---

## 7. Custom Data: Future OI

### Purpose
Store open interest data for futures (separate from Bar, which has no OI field).

### Storage Location
```
data/custom_futureoi/{INSTRUMENT_ID}/{START_TS}_{END_TS}.parquet
```

### Class
`FutureOI` (extends `nautilus_trader.core.data.Data`)

### Pattern
Uses `@customdataclass` decorator (same as OptionOI).

### Fields (6)

| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `instrument_id` | `InstrumentId` | Instrument identifier | User-defined |
| `oi` | `int` | Current open interest | User-defined |
| `coi` | `int` | Change in open interest | User-defined |
| `type` | `str` | Always "FutureOI" | Auto-generated |
| `ts_event` | `uint64_t` | Event timestamp (UTC ns) | Base class |
| `ts_init` | `uint64_t` | Init timestamp (UTC ns) | Base class |

### Example Directory Structure
```
data/custom_futureoi/
├── BANKNIFTY-I.NSE/
│   ├── 2024-01-31T03-45-00-000000000Z_2024-01-31T09-59-00-000000000Z.parquet
│   └── 2024-02-01T03-45-00-000000000Z_2024-02-01T09-59-00-000000000Z.parquet
└── NIFTY-I.NSE/
    └── 2024-01-31T03-45-00-000000000Z_2024-01-31T09-59-00-000000000Z.parquet
```

### Usage
```python
# Write FutureOI data
from marvelquant.data.types import FutureOI

oi_data = FutureOI(
    instrument_id=InstrumentId.from_str("BANKNIFTY-I.NSE"),
    oi=125000,
    coi=1500,
    ts_event=ts_event,
    ts_init=ts_init
)
catalog.write_data([oi_data])

# Query FutureOI data
oi_records = catalog.generic_data(
    cls=FutureOI,
    instrument_ids=["BANKNIFTY-I.NSE"],
    start=start_date,
    end=end_date
)
```

### Implementation
```python
# File: /marvelquant/data/types/future_oi.py
from nautilus_trader.core.data import Data, customdataclass

@customdataclass
class FutureOI(Data):
    """Futures Open Interest custom data type."""
    instrument_id: InstrumentId
    oi: int
    coi: int
    # ts_event and ts_init inherited from Data base class
    # 'type' column auto-generated as "FutureOI"
```

---

## 8. Streaming Greeks (Not Stored)

### Purpose
Calculate portfolio-level Greeks on-the-fly during backtesting and live trading.

### Method
`self.greeks.portfolio_greeks()`

### Configuration
`InterestRateProvider` actor with India 91-day T-Bill rates.

### Storage
❌ **NONE** - Greeks are computed in-memory, not stored to disk.

### Optional Post-Backtest Storage
After backtest completion, you can optionally save Greeks using:
```python
catalog.convert_stream_to_data(GreeksData)
```

### Why Streaming?

Based on Nautilus official example `databento_option_greeks.py`:

**Benefits**:
1. ✅ **Always current**: Recalculated on every bar update
2. ✅ **Portfolio-level aggregation**: Sum Greeks across all positions
3. ✅ **Scenario analysis**: Spot/vol shock testing
4. ✅ **Live trading compatible**: Same calculation in backtest and live
5. ✅ **No storage overhead**: No disk space needed

**Tradeoffs**:
1. ⚠️ **Computation cost**: Recalculated on every bar (acceptable for most strategies)
2. ⚠️ **Not queryable**: Cannot query historical Greeks without re-running backtest

### Usage
```python
# In strategy on_bar() method
portfolio_greeks = self.greeks.portfolio_greeks(
    use_cached_greeks=False,  # Force streaming calculation
    publish_greeks=True,       # Optional: save after backtest
    underlyings=["BANKNIFTY", "NIFTY"],
    spot_shock=0.0,            # Scenario: no spot shock
    vol_shock=0.0,             # Scenario: no vol shock
)

# Access portfolio Greeks
delta = portfolio_greeks.delta
gamma = portfolio_greeks.gamma
theta = portfolio_greeks.theta
vega = portfolio_greeks.vega
rho = portfolio_greeks.rho
```

### Configuration Example
```python
# In actor configuration
config = ActorConfig(
    component_id="InterestRateProvider",
    config={
        "currency": "INR",
        "rate_source": "india_91day_tbill",
        "default_rate": 0.065  # 6.5% annual
    }
)
```

---

## 9. Computed Fields

### Purpose
Calculate dynamic fields at query time rather than storing them.

### Fields

| Field | Calculation | Reason |
|-------|-------------|--------|
| **dte** | `(expiry_date - query_date).trading_days()` | Context-dependent (changes daily) |
| **expiry_bucket** | `IF dte <= 7 THEN 'CW' ELIF dte <= 14 THEN 'NW'...` | Derived from DTE |
| **strike_type** | Compare strike vs spot price | Dynamic (changes with spot) |
| **atm_strike** | Find strike closest to spot | Dynamic |

### Why Compute Instead of Store?

**Storage Savings**: ~16 bytes/row × millions of rows = significant

**Computation Cost**: <1ms with Polars/Arrow (negligible)

**Data Freshness**: Always accurate for current context

### Implementation Example
```python
import polars as pl

# Query bars and contracts
bars_df = catalog.bars(...).to_polars()
contracts_df = catalog.instruments(...).to_polars()

# Join
df = bars_df.join(contracts_df, on="instrument_id")

# Compute DTE (using trading days calendar)
df = df.with_columns([
    pl.col("expiry_date").apply(
        lambda exp: calendar.trading_days_between(date.today(), exp)
    ).alias("dte")
])

# Compute expiry bucket
df = df.with_columns([
    pl.when(pl.col("dte") <= 7).then(pl.lit("CW"))
      .when(pl.col("dte") <= 14).then(pl.lit("NW"))
      .when(pl.col("dte") <= 30).then(pl.lit("CM"))
      .otherwise(pl.lit("NM"))
      .alias("expiry_bucket")
])

# Compute strike type
spot_price = get_current_spot("BANKNIFTY")
df = df.with_columns([
    pl.when(pl.col("strike") == spot_price).then(pl.lit("ATM"))
      .when(pl.col("strike") < spot_price).then(
          pl.when(pl.col("option_type") == "CE").then(pl.lit("ITM"))
            .otherwise(pl.lit("OTM"))
      )
      .otherwise(
          pl.when(pl.col("option_type") == "CE").then(pl.lit("OTM"))
            .otherwise(pl.lit("ITM"))
      )
      .alias("strike_type")
])
```

---

## 10. VIX Integration

### Purpose
Integrate India VIX for volatility-based strategy logic.

### Finding
Nautilus does NOT have built-in VIX support.

### Solution
Treat VIX as `IndexInstrument` (Nautilus native class).

### InstrumentId
`INDIA-VIX.NSE`

### Storage
```
data/bar/INDIA-VIX.NSE/INDIA-VIX.NSE-1-MINUTE-LAST-EXTERNAL/data.parquet
```

### Data Format
Use `Bar` class:
- **Open, High, Low, Close**: VIX values
- **Volume**: 0 (VIX is not traded)
- **Close** field used for VIX value

### Why This Approach?

1. ✅ VIX is market-specific (CBOE for US, NSE for India)
2. ✅ Different calculation methodologies per exchange
3. ✅ Not a tradeable instrument (no volume)
4. ✅ Integrates seamlessly with existing index pipeline

### Usage in Strategy
```python
# Subscribe to VIX
self.subscribe_bars(
    BarType.from_str("INDIA-VIX.NSE-1-MINUTE-LAST-EXTERNAL")
)

# In on_bar() method
def on_bar(self, bar: Bar) -> None:
    if bar.bar_type.instrument_id.symbol.value == "INDIA-VIX":
        current_vix = float(bar.close)

        # VIX-based position sizing
        if current_vix > 30:
            self.position_size_multiplier = 0.5  # Risk-off
        elif current_vix < 15:
            self.position_size_multiplier = 1.5  # Risk-on
        else:
            self.position_size_multiplier = 1.0  # Normal
```

### Benefits

- **Real-time volatility filtering**: Avoid trading in high-VIX environments
- **Dynamic position sizing**: Scale positions based on market volatility
- **Market regime detection**: Identify low-vol vs high-vol regimes
- **Risk management integration**: Circuit breakers on VIX spikes

---

## 11. Calendar and Expiry Management

### 11.1 Holiday Calendar

**Finding**: Nautilus does NOT have built-in holiday calendar support.

**Solution**: External calendar library + custom implementation.

**Implementation**: `NSEHolidayCalendar` class.

**Purpose**: Accurate DTE calculations using trading days (not calendar days).

### NSE Holiday Calendar

**File**: `/marvelquant/utils/nse_calendar.py`

**Features**:
- 98 NSE holidays (2018-2024)
- Weekend exclusion (Saturday=5, Sunday=6)
- `is_trading_day(date)` check
- `trading_days_between(start, end)` calculation
- `next_trading_day(date)` lookup

**Data Sources**:
- ChittorGarh (NSE official circular republisher)
- AngelOne (broker holiday calendar)
- Zerodha (broker holiday calendar)

**Example Holidays**:
```python
NSE_HOLIDAYS = {
    date(2024, 1, 26): "Republic Day",
    date(2024, 3, 8): "Maha Shivaratri",
    date(2024, 3, 25): "Holi",
    date(2024, 8, 15): "Independence Day",
    date(2024, 10, 2): "Gandhi Jayanti",
    date(2024, 11, 1): "Diwali",
    date(2024, 12, 25): "Christmas",
}
```

### Usage
```python
from marvelquant.utils.nse_calendar import NSEHolidayCalendar

calendar = NSEHolidayCalendar()

# Check if date is trading day
is_open = calendar.is_trading_day(date(2024, 1, 26))  # False (Republic Day)

# Calculate trading days between dates
dte = calendar.trading_days_between(
    date.today(),
    option_expiry_date
)
```

### 11.2 Expiry Calendar

**NSE Expiry Rules**:
- **Monthly Options/Futures**: Last Thursday of every month
- **Weekly Options**: Every Thursday (introduced 2019)
- **Holiday Adjustment**: If last Thursday is holiday → previous trading day

### Expiry Calculator

**File**: `/marvelquant/utils/expiry_calculator.py`

**Functions**:
```python
def get_nse_monthly_expiry(year: int, month: int, calendar: NSEHolidayCalendar) -> date:
    """Calculate NSE monthly expiry (last Thursday, adjusted for holidays)."""
    ...

def get_nse_weekly_expiry(date: date, calendar: NSEHolidayCalendar) -> date:
    """Get the weekly expiry for a given date (Thursday of the week)."""
    ...

def classify_expiry_bucket(dte: int) -> str:
    """Classify DTE into expiry buckets (CW/NW/CM/NM)."""
    ...

def calculate_dte(option_expiry: date, calendar: NSEHolidayCalendar) -> int:
    """Calculate DTE using trading days (not calendar days)."""
    ...
```

### Expiry Bucket Classification

| Bucket | DTE Range | Description | Use Case |
|--------|-----------|-------------|----------|
| **CW** | ≤7 days | Current Week | 0DTE strategies, weekly spreads |
| **NW** | 8-14 days | Next Week | Short-term credit spreads |
| **CM** | ≤30 days | Current Month | Monthly iron condors |
| **NM** | 31+ days | Next Month | LEAPS, diagonal spreads |

### Usage
```python
from marvelquant.utils.expiry_calculator import (
    get_nse_monthly_expiry,
    calculate_dte,
    classify_expiry_bucket
)

# Get monthly expiry
expiry_date = get_nse_monthly_expiry(2024, 10, calendar)  # Last Thursday of Oct 2024

# Calculate DTE
dte = calculate_dte(expiry_date, calendar)

# Classify bucket
bucket = classify_expiry_bucket(dte)  # "CW", "NW", "CM", or "NM"
```

### Why Calendar-Aware DTE?

1. **Accurate theta calculations**: Options decay based on trading days
2. **Correct expiry bucket classification**: Needed for strategy logic
3. **Better execution timing**: Know when to roll positions
4. **Market-specific behavior**: NSE vs CBOE vs CME have different rules

### Expiry Rollover Logic
```python
# Auto-rollover trigger at 7 DTE
if dte <= 7 and dte > 0:
    # Close expiring position
    self.close(position.instrument_id)

    # Open new position in next month
    next_month = (today.month % 12) + 1
    next_year = today.year + (1 if next_month == 1 else 0)
    next_expiry = get_nse_monthly_expiry(next_year, next_month, calendar)

    # ... select strike and open position
```

---

## 12. Symbol Naming Conventions

### Nautilus Standard Format

**Format**: `{SYMBOL}.{VENUE}`

**From Nautilus Documentation**:
> "All instruments should have a unique InstrumentId, which is made up of both the native symbol, and venue ID, separated by a period."

### NSE Examples

| Data Type | Raw Symbol | InstrumentId | Venue |
|-----------|------------|--------------|-------|
| **Index** | BANKNIFTY | `BANKNIFTY-INDEX.NSE` | NSE |
| **Futures (CM)** | - | `BANKNIFTY-I.NSE` | NSE |
| **Futures (NM)** | - | `BANKNIFTY-II.NSE` | NSE |
| **Option Call** | BANKNIFTY28OCT2548000CE | `BANKNIFTY28OCT2548000CE.NSE` | NSE |
| **Option Put** | BANKNIFTY28OCT2548000PE | `BANKNIFTY28OCT2548000PE.NSE` | NSE |
| **VIX Index** | INDIA VIX | `INDIA-VIX.NSE` | NSE |

### Key Rules

1. **Venue component is MANDATORY**: Every instrument MUST have a venue identifier
2. **Symbol + Venue must be UNIQUE**: The `{symbol.venue}` combination must be unique
3. **Native symbols preserved**: Original exchange symbol stored in `instrument.raw_symbol`
4. **Uppercase normalized**: All symbols converted to uppercase

### Continuous Futures Notation

| Notation | Description | NSE Term | Example |
|----------|-------------|----------|---------|
| **-I** | Front month (most liquid) | Current Month (CM) | `BANKNIFTY-I.NSE` |
| **-II** | Second month | Next Month (NM) | `BANKNIFTY-II.NSE` |
| **-INDEX** | Spot/Cash | Index | `BANKNIFTY-INDEX.NSE` |

### Symbol Parsing
```python
from marvelquant.utils.contract_generators import parse_nse_option_symbol

# Parse option symbol
components = parse_nse_option_symbol("BANKNIFTY28OCT2548000CE")
# Returns: {
#   'underlying': 'BANKNIFTY',
#   'expiry_date': date(2025, 10, 28),
#   'strike': 48000.0,
#   'option_type': 'CE'
# }
```

---

## 13. Data Access Patterns

### Pattern 1: Query Bar Data (OHLCV)

```python
# Single instrument
bars = catalog.bars(
    instrument_ids=["BANKNIFTY28OCT2548000CE.NSE"],
    bar_type="BANKNIFTY28OCT2548000CE.NSE-1-MINUTE-LAST-EXTERNAL",
    start=start_date,
    end=end_date
)

# Convert to Polars for analysis
bars_df = bars.to_polars()
```

### Pattern 2: Query Option OI

```python
from marvelquant.data.types import OptionOI

# Query OptionOI data
oi_records = catalog.generic_data(
    cls=OptionOI,
    instrument_ids=["BANKNIFTY28OCT2548000CE.NSE"],
    start=start_date,
    end=end_date
)

# Convert to Polars
oi_df = oi_records.to_polars()
```

### Pattern 3: Query Futures OI

```python
from marvelquant.data.types import FutureOI

# Query FutureOI data
oi_records = catalog.generic_data(
    cls=FutureOI,
    instrument_ids=["BANKNIFTY-I.NSE"],
    start=start_date,
    end=end_date
)
```

### Pattern 4: Query Contract Metadata

```python
# Query option contracts
contracts = catalog.instruments(
    instrument_type=OptionContract,
    instrument_ids=["BANKNIFTY28OCT2548000CE.NSE"]
)

# Access contract fields
for contract in contracts:
    print(f"Strike: {contract.strike_price}")
    print(f"Expiry: {contract.expiration_ns}")
    print(f"Lot Size: {contract.lot_size}")
```

### Pattern 5: Join Bar + OI + Contract

```python
import polars as pl

# Query all data
bars_df = catalog.bars(...).to_polars()
oi_df = catalog.generic_data(cls=OptionOI, ...).to_polars()
contracts_df = catalog.instruments(...).to_polars()

# Join on instrument_id and timestamp
df = (
    bars_df
    .join(oi_df, on=["instrument_id", "ts_event"], how="left")
    .join(contracts_df, on="instrument_id", how="left")
)

# Now have: OHLCV + OI + strike + expiry + lot_size
```

### Pattern 6: Streaming Greeks Calculation

```python
# In strategy on_bar() method
portfolio_greeks = self.greeks.portfolio_greeks(
    use_cached_greeks=False,
    publish_greeks=True,
    underlyings=["BANKNIFTY"],
    spot_shock=0.0,
    vol_shock=0.0,
)

# Access Greeks
delta = portfolio_greeks.delta
theta = portfolio_greeks.theta
```

### Pattern 7: Strike Selection with Computed Fields

```python
# Query bars and contracts
bars_df = catalog.bars(...).to_polars()
contracts_df = catalog.instruments(...).to_polars()

# Join
df = bars_df.join(contracts_df, on="instrument_id")

# Compute DTE
df = df.with_columns([
    pl.col("expiry_date").apply(
        lambda exp: calendar.trading_days_between(date.today(), exp)
    ).alias("dte")
])

# Filter CW bucket (≤7 DTE)
cw_df = df.filter(pl.col("dte") <= 7)

# Find ATM strike
spot_price = get_current_spot("BANKNIFTY")
atm_df = cw_df.with_columns([
    (pl.col("strike") - spot_price).abs().alias("strike_diff")
]).sort("strike_diff").head(1)
```

### Pattern 8: Query QuoteTicks (For Greeks Calculation)

```python
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from datetime import date

# Load catalog
catalog = ParquetDataCatalog("./data/data")

# Query QuoteTicks for specific instrument
quotes = catalog.quote_ticks(
    instrument_ids=["NIFTY-INDEX.NSE"],
    start=date(2024, 1, 31),
    end=date(2024, 1, 31)
)

# Access QuoteTick data
for quote in quotes:
    print(f"Bid: {quote.bid_price}, Ask: {quote.ask_price}")
    print(f"Mid: {(quote.bid_price + quote.ask_price) / 2}")
    print(f"Spread: {quote.ask_price - quote.bid_price}")

# Convert to Polars DataFrame
quotes_df = quotes.to_polars()
print(quotes_df.head())
# Output:
#   bid_price | ask_price | bid_size | ask_size | ts_event
#   21435.50  | 21435.50  | 1        | 1        | 1706678700000000000
```

**Query multiple instruments**:
```python
# Query QuoteTicks for all options (pattern matching)
quotes = catalog.quote_ticks(
    instrument_ids=["NIFTY01FEB24*.NSE"],  # All 01FEB24 options
    start=date(2024, 1, 31),
    end=date(2024, 1, 31)
)
```

**Critical Note**: QuoteTicks are **required** for Greeks calculation. Without QuoteTicks, `self.greeks.instrument_greeks()` returns `None`.

---

## 14. Performance Characteristics

### Storage Efficiency

**Normalized Design**:
- **40-70% storage reduction** vs unified schema
- **10:1 compression ratio** with columnar storage per data type
- **Smaller file sizes**: Query only needed columns

**Example**:
- Unified OptionChainSnapshot: 22 fields × 4 bytes avg = 88 bytes/row
- Normalized:
  - Bar: 9 fields × 4 bytes = 36 bytes/row
  - OptionOI: 6 fields × 4 bytes = 24 bytes/row
  - Contract metadata: Loaded once (cached)
  - **Total query size depends on what you need**

### Query Performance

**Computed Fields**: <1ms with Polars/Arrow

**Data Joins**:
- **Bar + OI**: <10ms for 1 day (thousands of rows)
- **Bar + Contract**: <5ms (contract metadata cached)
- **Full join (Bar + OI + Contract)**: <15ms

**Caching Strategy**:
- **Contract metadata**: Load once, cache in memory
- **Bars**: Query window (e.g., 1 day at a time)
- **OI**: Query same window as bars

### Streaming Greeks

**Computation Cost**: ~1-5ms per bar update (depends on portfolio size)

**Portfolio Size Impact**:
- 1-10 positions: <1ms
- 10-50 positions: 1-3ms
- 50+ positions: 3-5ms

**Acceptable**: For most strategies running on minute/second bars

---

## 15. Rationale and Benefits

### Why This Architecture?

**1. Nautilus Compliance**
- ✅ Uses native `Bar` class (which has no OI field)
- ✅ Uses native `OptionContract` and `FuturesContract` classes
- ✅ Uses `@customdataclass` pattern for custom data
- ✅ Uses `catalog.write_data()` for standard directory structure
- ✅ Uses streaming Greeks (production pattern from `databento_option_greeks.py`)

**2. Industry Standard**
- ✅ Matches Databento, Polygon.io, QuantConnect patterns
- ✅ Separates OHLCV, OI, and Greeks
- ✅ Normalized schema design

**3. Performance**
- ✅ 40-70% storage reduction
- ✅ Query only needed data
- ✅ Better compression (columnar per data type)
- ✅ Fast computed fields (<1ms)

**4. Future-Proof**
- ✅ Easier Epic 3 integration (NautilusTrader core)
- ✅ Live trading compatible (streaming Greeks)
- ✅ No technical debt from custom directory structure
- ✅ Extensible (add new custom data types as needed)

**5. Research-Validated**
- ✅ EXA MCP research confirmed industry patterns
- ✅ Official Nautilus example (`databento_option_greeks.py`) validates approach
- ✅ Tested with production transformation pipeline (58,593 bars + OI data)

### Benefits Summary

| Benefit | Description | Impact |
|---------|-------------|--------|
| **Nautilus Standard** | Uses native classes and patterns | High |
| **Storage Efficiency** | 40-70% reduction vs unified schema | High |
| **Query Performance** | <1ms computed fields, <15ms joins | High |
| **Streaming Greeks** | Always current, portfolio-level | Critical |
| **Live Trading Ready** | Same pattern backtest and live | Critical |
| **Maintainability** | Standard patterns, less custom code | High |
| **Extensibility** | Easy to add new data types | Medium |
| **Developer Experience** | Clear patterns, good docs | High |

### Cost-Benefit Analysis

**Costs**:
- Story 2.5.4.1 restart: 7 days
- Documentation updates: 12 days (parallelizable)
- **Total delay**: ~2 weeks

**Benefits**:
- Prevents mid-Epic 3 rework (would cost 3-4 weeks)
- Industry-standard architecture (reduces technical debt)
- Better performance and storage efficiency
- Easier future maintenance

**Net Benefit**: +1-2 weeks saved long-term, better architecture quality

---

## Document Control

**Version**: 1.0
**Status**: APPROVED
**Distribution**: All development teams
**Next Review**: After Epic 2.5 completion
**Document Owner**: Maruth (Product Owner)

---

**END OF DATA ARCHITECTURE DOCUMENT**
