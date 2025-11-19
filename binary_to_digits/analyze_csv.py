import pandas as pd
import struct
import ast
from datetime import datetime

def decode_binary_value(binary_str):
    """Decode binary string representation to numeric value."""
    try:
        # Parse the string representation of bytes
        if isinstance(binary_str, str):
            # Handle quoted strings
            if binary_str.startswith('"') and binary_str.endswith('"'):
                binary_str = binary_str[1:-1]
            
            # Convert string representation to actual bytes
            byte_data = ast.literal_eval(binary_str)
            
            # Interpret as little-endian 64-bit integer
            value = struct.unpack('<Q', byte_data)[0]
            return value
    except Exception as e:
        return None

def timestamp_to_datetime(ts):
    """Convert nanosecond timestamp to datetime."""
    try:
        return datetime.fromtimestamp(ts / 1e9)
    except:
        return None

# Read the CSV file
print("=" * 80)
print("CSV FILE ANALYSIS")
print("=" * 80)
print()

df = pd.read_csv('2011-04-01T03-45-59-000000000Z_2011-04-01T09-59-59-000000000Z.csv')

print(f"Total rows: {len(df)}")
print(f"Columns: {', '.join(df.columns)}")
print()

# Decode the binary columns
print("Decoding binary data...")
print()

for col in ['open', 'high', 'low', 'close', 'volume']:
    df[f'{col}_decoded'] = df[col].apply(decode_binary_value)

# Convert timestamps
df['ts_event_dt'] = df['ts_event'].apply(timestamp_to_datetime)
df['ts_init_dt'] = df['ts_init'].apply(timestamp_to_datetime)

# Display basic statistics
print("=" * 80)
print("DATA OVERVIEW")
print("=" * 80)
print()

print("First 5 rows (decoded):")
print(df[['open_decoded', 'high_decoded', 'low_decoded', 'close_decoded', 
         'volume_decoded', 'ts_event_dt']].head())
print()

print("Last 5 rows (decoded):")
print(df[['open_decoded', 'high_decoded', 'low_decoded', 'close_decoded', 
         'volume_decoded', 'ts_event_dt']].tail())
print()

# Statistical summary
print("=" * 80)
print("STATISTICAL SUMMARY")
print("=" * 80)
print()

print("Price Statistics (in raw units):")
for col in ['open', 'high', 'low', 'close']:
    decoded_col = f'{col}_decoded'
    print(f"\n{col.upper()}:")
    print(f"  Min:    {df[decoded_col].min():,.0f}")
    print(f"  Max:    {df[decoded_col].max():,.0f}")
    print(f"  Mean:   {df[decoded_col].mean():,.2f}")
    print(f"  Median: {df[decoded_col].median():,.0f}")

print(f"\nVOLUME:")
print(f"  Min:    {df['volume_decoded'].min():,.0f}")
print(f"  Max:    {df['volume_decoded'].max():,.0f}")
print(f"  Mean:   {df['volume_decoded'].mean():,.2f}")
print(f"  Total:  {df['volume_decoded'].sum():,.0f}")

# Time range
print()
print("=" * 80)
print("TIME RANGE")
print("=" * 80)
print()
print(f"Start: {df['ts_event_dt'].min()}")
print(f"End:   {df['ts_event_dt'].max()}")
print(f"Duration: {df['ts_event_dt'].max() - df['ts_event_dt'].min()}")
print()

# Calculate price changes
df['price_change'] = df['close_decoded'] - df['open_decoded']
df['price_change_pct'] = (df['price_change'] / df['open_decoded']) * 100

print("=" * 80)
print("PRICE MOVEMENT ANALYSIS")
print("=" * 80)
print()
print(f"Rows with price increase: {(df['price_change'] > 0).sum()} ({(df['price_change'] > 0).sum() / len(df) * 100:.1f}%)")
print(f"Rows with price decrease: {(df['price_change'] < 0).sum()} ({(df['price_change'] < 0).sum() / len(df) * 100:.1f}%)")
print(f"Rows with no change:      {(df['price_change'] == 0).sum()} ({(df['price_change'] == 0).sum() / len(df) * 100:.1f}%)")
print()
print(f"Largest price increase:  {df['price_change'].max():,.0f} ({df['price_change_pct'].max():.2f}%)")
print(f"Largest price decrease:  {df['price_change'].min():,.0f} ({df['price_change_pct'].min():.2f}%)")
print()

# Data quality checks
print("=" * 80)
print("DATA QUALITY")
print("=" * 80)
print()
print(f"Missing values: {df.isnull().sum().sum()}")
print(f"Duplicate timestamps: {df['ts_event'].duplicated().sum()}")
print()

# Check for anomalies
print("Checking for anomalies...")
same_ohlc = (df['open_decoded'] == df['high_decoded']) & \
            (df['high_decoded'] == df['low_decoded']) & \
            (df['low_decoded'] == df['close_decoded'])
print(f"Rows where OHLC are all the same: {same_ohlc.sum()} ({same_ohlc.sum() / len(df) * 100:.1f}%)")
print()

# Save decoded data
output_file = '2011-04-01_decoded.csv'
df.to_csv(output_file, index=False)
print(f"Decoded data saved to: {output_file}")
print()

print("=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)

