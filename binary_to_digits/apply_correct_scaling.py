import pandas as pd

# Read the converted file
df = pd.read_csv('converted_to_digits.csv')

print("Applying scaling factor (dividing by 1,000,000,000)...")
print()

# Apply scaling - divide prices by 1 billion, volume by 1 billion
df['open'] = df['open'] / 1_000_000_000
df['high'] = df['high'] / 1_000_000_000
df['low'] = df['low'] / 1_000_000_000
df['close'] = df['close'] / 1_000_000_000
df['volume'] = df['volume'] / 1_000_000_000

# Save the properly scaled file
output_file = 'converted_to_digits_scaled.csv'
df.to_csv(output_file, index=False)

print(f"✓ Saved to: {output_file}")
print()
print("First 10 rows with proper scaling:")
print("=" * 80)
print(df[['open', 'high', 'low', 'close', 'volume']].head(10).to_string())
print()
print("=" * 80)
print(f"\nPrice range: ${df['close'].min():.2f} to ${df['close'].max():.2f}")
print(f"Average price: ${df['close'].mean():.2f}")
print()
print("These look like Bitcoin prices from April 2011! 🎯")

