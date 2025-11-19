# Directory Structure Analysis: raw/ vs Nautilus.py Expectations

## Current Actual Structure

```
raw/
└── parquet_data/
    ├── cash/
    │   └── nifty/
    │       └── 2025/
    │           └── 11/
    │               └── nifty_cash_20251103.parquet
    ├── futures/
    │   └── nifty/
    │       └── 2025/
    │           └── 11/
    │               └── nifty_future_20251103.parquet
    └── options/
        ├── nifty_call/
        │   └── 2025/
        │       └── 11/
        │           └── nifty_call_20251103.parquet
        └── nifty_put/
            └── 2025/
                └── 11/
                    └── nifty_put_20251103.parquet
```

## What Nautilus.py Expects

Based on the code analysis, `Nautilus.py` expects the following structure:

### Expected Structure:
```
<input_dir>/
├── cash/          (or "index/")
│   └── nifty/
│       └── *.parquet files (can be nested in subdirectories)
├── futures/
│   └── nifty/
│       └── *.parquet files (can be nested in subdirectories)
└── options/       (or "option/")
    ├── nifty_call/
    │   └── *.parquet files (can be nested in subdirectories)
    └── nifty_put/
        └── *.parquet files (can be nested in subdirectories)
```

### Code Expectations:

1. **Index/Cash Data** (lines 219-225, 331-357):
   - Looks for: `input_dir/cash/nifty/` or `input_dir/index/nifty/`
   - Uses `rglob("*.parquet")` to find all parquet files recursively
   - ✅ **Year/month nesting is OK** (rglob handles this)

2. **Futures Data** (lines 459-483):
   - Looks for: `input_dir/futures/nifty/`
   - Uses `rglob("*.parquet")` to find all parquet files recursively
   - Filters for files matching: `nifty_future_YYYYMMDD.parquet`
   - ✅ **Year/month nesting is OK** (rglob handles this)

3. **Options Data** (lines 228-249, 595-633):
   - Looks for: `input_dir/options/nifty_call/` or `input_dir/options/nifty_put/`
   - Uses `rglob("*.parquet")` to find all parquet files recursively
   - Filters for files matching: `nifty_call_YYYYMMDD.parquet` or `nifty_put_YYYYMMDD.parquet`
   - ✅ **Year/month nesting is OK** (rglob handles this)

## The Mismatch

### Problem:
Your structure has an **extra `parquet_data/` level** that the code doesn't account for.

- **Code expects**: `raw/cash/nifty/...`
- **Actual structure**: `raw/parquet_data/cash/nifty/...`

### Impact:
If you run:
```bash
python Nautilus.py --input-dir raw
```

The code will:
1. Look for `raw/cash/nifty/` → ❌ **NOT FOUND**
2. Look for `raw/futures/nifty/` → ❌ **NOT FOUND**
3. Look for `raw/options/nifty_call/` → ❌ **NOT FOUND**

The fallback mechanism (line 171-174) will try to scan all parquet files recursively, but it may not properly categorize them as index/futures/options.

## Solutions

### Solution 1: Use `parquet_data` as input directory (RECOMMENDED)
```bash
python Nautilus.py --input-dir raw/parquet_data
```

This will work because:
- `raw/parquet_data/cash/nifty/` → ✅ Found
- `raw/parquet_data/futures/nifty/` → ✅ Found
- `raw/parquet_data/options/nifty_call/` → ✅ Found

### Solution 2: Modify Nautilus.py to handle `parquet_data/` subdirectory
Add support for an optional `parquet_data/` prefix in the directory search functions.

### Solution 3: Restructure your data
Move files from `raw/parquet_data/` to `raw/` directly (not recommended if you want to keep the structure).

## File Naming Requirements

The code has specific file naming requirements:

1. **Futures files** (line 477):
   - Must match: `{symbol}_future_YYYYMMDD.parquet`
   - Example: `nifty_future_20251103.parquet` ✅

2. **Options files** (lines 624-625):
   - Must match: `{symbol}_call_YYYYMMDD.parquet` or `{symbol}_put_YYYYMMDD.parquet`
   - Example: `nifty_call_20251103.parquet` ✅
   - Example: `nifty_put_20251103.parquet` ✅

3. **Index/Cash files**:
   - No specific naming requirement (any `.parquet` file in the directory)
   - Your file: `nifty_cash_20251103.parquet` ✅

## Verification Checklist

- ✅ Year/month subdirectories are supported (rglob handles recursion)
- ✅ File naming matches requirements
- ⚠️ **Need to use `raw/parquet_data` as input directory** (not `raw`)

## Recommended Command

```bash
python Nautilus.py --input-dir raw/parquet_data --symbols NIFTY
```

Or to auto-discover symbols:
```bash
python Nautilus.py --input-dir raw/parquet_data --symbols AUTO
```

