import pandas as pd
import struct
import ast
import sys
from datetime import datetime

def decode_binary(binary_str):
    """Convert binary string to integer."""
    try:
        if isinstance(binary_str, str):
            # Remove quotes if present
            if binary_str.startswith('"') and binary_str.endswith('"'):
                binary_str = binary_str[1:-1]
            
            # Convert string representation to bytes
            byte_data = ast.literal_eval(binary_str)
            
            # Unpack as 64-bit integer
            value = struct.unpack('<Q', byte_data)[0]
            return value
    except:
        return None

def nanoseconds_to_datetime(ns_timestamp):
    """Convert nanoseconds timestamp to human-readable datetime string."""
    try:
        # Convert nanoseconds to seconds
        seconds = ns_timestamp / 1e9
        # Create datetime object
        dt = datetime.fromtimestamp(seconds)
        # Return formatted string
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return None

# Get input file path from command line or use default
if len(sys.argv) > 1:
    input_file = sys.argv[1]
else:
    input_file = '2025_10_06T03_51_00_000000000Z_2025_10_06T08_53_00_000000000Z.csv'

# Read the original CSV
print(f"Reading CSV file: {input_file}")
df = pd.read_csv(input_file)

# Convert binary columns to digits
print("Converting binary data to digits...")
df['open'] = df['open'].apply(decode_binary)
df['high'] = df['high'].apply(decode_binary)
df['low'] = df['low'].apply(decode_binary)
df['close'] = df['close'].apply(decode_binary)
df['volume'] = df['volume'].apply(decode_binary)

print("\nOriginal large numbers (first 5 rows):")
print(df[['open', 'high', 'low', 'close', 'volume']].head())
print()

# Apply scaling - divide by 1 billion for proper values
print("Applying scaling factor (dividing by 1,000,000,000)...")
df['open'] = df['open'] / 1_000_000_000
df['high'] = df['high'] / 1_000_000_000
df['low'] = df['low'] / 1_000_000_000
df['close'] = df['close'] / 1_000_000_000
df['volume'] = df['volume'] / 1_000_000_000

# Convert timestamps to human-readable format
print("Converting timestamps to human-readable format...")
df['ts_event_readable'] = df['ts_event'].apply(nanoseconds_to_datetime)
df['ts_init_readable'] = df['ts_init'].apply(nanoseconds_to_datetime)

# Reorder columns to put readable timestamps after the original ones
columns_order = ['open', 'high', 'low', 'close', 'volume', 
                 'ts_event', 'ts_event_readable', 
                 'ts_init', 'ts_init_readable']
df = df[columns_order]

# Save the converted and scaled file
output_file = 'converted_to_digits_with_time.csv'
df.to_csv(output_file, index=False)

print(f"\n✓ Done! Saved to: {output_file}")
print()
print("First 10 rows with proper scaling:")
print("=" * 80)
print(df[['open', 'high', 'low', 'close', 'volume', 'ts_event_readable']].head(10).to_string())
print()
print("=" * 80)
print(f"\nPrice range: ${df['close'].min():.2f} to ${df['close'].max():.2f}")
print(f"Average price: ${df['close'].mean():.2f}")
print(f"Total rows converted: {len(df)}")
print(f"\nTime range: {df['ts_event_readable'].iloc[0]} to {df['ts_event_readable'].iloc[-1]}")
