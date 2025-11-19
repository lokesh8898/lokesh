import pandas as pd

# Read the converted file
df = pd.read_csv('converted_to_digits.csv')

print("Original large numbers (first 5 rows):")
print(df[['open', 'high', 'low', 'close', 'volume']].head())
print()

# Try different scaling factors to see what makes sense
print("Testing different scaling factors:")
print("=" * 80)

# Common scaling factors for financial data
scaling_factors = {
    'Divide by 100 (cents to dollars)': 100,
    'Divide by 1,000': 1000,
    'Divide by 10,000': 10000,
    'Divide by 100,000': 100000,
    'Divide by 1,000,000 (millions)': 1000000,
    'Divide by 10,000,000': 10000000,
    'Divide by 100,000,000': 100000000,
    'Divide by 1,000,000,000 (billions)': 1000000000,
}

sample_open = df['open'].iloc[0]
sample_volume = df['volume'].iloc[0]

print(f"\nOriginal open price: {sample_open:,}")
print(f"Original volume: {sample_volume:,}")
print()

for name, factor in scaling_factors.items():
    scaled_price = sample_open / factor
    scaled_volume = sample_volume / factor
    print(f"{name:40} → Price: {scaled_price:15,.2f}  Volume: {scaled_volume:15,.0f}")

print()
print("=" * 80)
print("\nWhich scaling looks right to you?")
print("Common patterns:")
print("  - Stock prices: usually $0.01 to $10,000")
print("  - Crypto (2011): Bitcoin was around $1-$30 in April 2011")
print("  - Forex: usually 0.0001 to 10.0000")

