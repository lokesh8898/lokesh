# GDFL Tick Data to CSV Processor

This is a modified version of the tick data processing system that **outputs processed data as CSV files** instead of uploading to a database.

## Overview

The system processes tick-by-tick trading data from GDFL files and converts them into cleaned, standardized CSV format without requiring any database connection.

## Directory Structure

```
parquet_conversion/
├── GFDLNFO_TICK_03012025/          # Input: Your GDFL data folder
│   ├── Futures/
│   │   └── -I/
│   │       └── NIFTY-I.csv
│   └── Options/
│       ├── NIFTY21JAN24000CE.csv
│       └── ...
│
├── ProcessedCSV/                    # Output: Processed CSV files
│   ├── nifty_future_03012025.csv
│   ├── nifty_call_03012025.csv
│   ├── nifty_put_03012025.csv
│   ├── banknifty_future_03012025.csv
│   └── ...
│
├── Logs/                            # Processing logs
│   └── 03012025, 143020.123456.log
│
├── config.py                        # Configuration file
├── prepareTickByTickData_CSV.py    # Core processing logic (no DB)
└── processToCSV.py                  # Main script to run
```

## Quick Start

### 1. Update Configuration

Edit `config.py` and set your input folder:

```python
GDFL_FILES_FOLDER = [
    r"GFDLNFO_TICK_03012025"  # Change this to your folder name
]
```

### 2. Run the Processor

```bash
python processToCSV.py
```

This will process all combinations of:
- **Indices**: NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX
- **Instrument Types**: call, put, future

### 3. Find Your Output

Processed CSV files will be in the `ProcessedCSV/` folder, named as:
- `{index}_{instrument_type}_{date}.csv`

Example: `nifty_call_03012025.csv`

## Output CSV Format

### For Futures:
```csv
date,time,symbol,open,high,low,close,volume,oi
250103,34500,NIFTY,2450000,2450000,2450000,2450000,100,5000
```

### For Options (Call/Put):
```csv
date,time,symbol,strike,expiry,open,high,low,close,volume,oi
250103,34500,NIFTY21JAN24000CE,24000.0,250103,15000,15000,15000,15000,50,1000
```

## Column Descriptions

| Column | Description | Type | Note |
|--------|-------------|------|------|
| date | Trading date | int | Format: YYMMDD |
| time | Time of tick | int | Seconds since midnight |
| symbol | Instrument symbol | text | |
| strike | Strike price | float | Options only |
| expiry | Expiry date | int | Options only, Format: YYMMDD |
| open | Open price | int | Multiplied by 100 |
| high | High price | int | Multiplied by 100 |
| low | Low price | int | Multiplied by 100 |
| close | Close/LTP | int | Multiplied by 100 |
| volume | Volume | int | |
| oi | Open Interest | int | |

**Note**: Prices are multiplied by 100 and stored as integers for efficiency. 
- Example: Price 245.50 → stored as 24550

## Data Processing Steps

The system performs the following transformations:

1. **Column Normalization**: Handles various column name variations
2. **Data Cleaning**: Removes null symbols, zero volume records, duplicates
3. **Time Filtering**: Keeps only market hours data (9:15 AM - 3:30 PM)
4. **Date/Time Conversion**: Converts to standardized integer formats
5. **Price Conversion**: Multiplies by 100 and converts to int32
6. **OHLC Creation**: Uses LTP for all OHLC values (tick data)

## Processing Individual Index/Instrument

To process a specific combination manually:

```bash
python prepareTickByTickData_CSV.py NIFTY call
python prepareTickByTickData_CSV.py BANKNIFTY future
python prepareTickByTickData_CSV.py SENSEX put
```

## Market Timings

Configured in `config.py`:

- **NFO** (NSE F&O): 9:15 AM - 3:30 PM
- **BFO** (BSE F&O): 9:15 AM - 3:30 PM

## Logs

All processing logs are saved in the `Logs/` folder with timestamps.

## Original vs CSV-Only Version

| Feature | Original | CSV-Only |
|---------|----------|----------|
| Database connection | Required (2 MySQL DBs) | Not required |
| Output | MySQL tables | CSV files |
| Metadata source | Remote DB | File names |
| Duplicate checking | DB query | N/A |
| Files | 3 files | 3 files |

## Troubleshooting

**No CSV files generated?**
- Check `Logs/` folder for errors
- Ensure GDFL folder structure is correct (Futures/-I/, Options/)
- Verify file names start with index name (e.g., NIFTY-I.csv)

**Empty CSV files?**
- Data may be filtered out due to market hours
- Check if source files have valid data

**Wrong folder processed?**
- Update `GDFL_FILES_FOLDER` in `config.py`

## Next Steps

After generating CSV files, you can:
1. Convert to Parquet format for better compression
2. Upload to cloud storage
3. Import into data analysis tools
4. Load into any database system of your choice

