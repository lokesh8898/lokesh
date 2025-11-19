import os
import time

print("Monitoring processing progress...\n")
print("=" * 70)

# Check ProcessedParquet folder
if os.path.exists("ProcessedParquet"):
    files = [f for f in os.listdir("ProcessedParquet") if f.endswith('.parquet')]
    files.sort()
    
    print(f"\n✅ GENERATED PARQUET FILES: {len(files)}")
    print("-" * 70)
    
    if files:
        for f in files:
            file_path = os.path.join("ProcessedParquet", f)
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"  {f:<40} {size_mb:>8.2f} MB")
    else:
        print("  (No files yet...)")
else:
    print("ProcessedParquet folder not found")

print("\n" + "=" * 70)
print("\nProcessing is running in the background...")
print("Run this script again to check updated progress!")

