# How to Specify Input Directories

You have **3 ways** to specify where your GDFL data is located:

---

## Method 1: Edit config.py (Recommended for Regular Use)

Open `config.py` and edit these lines:

```python
GDFL_FILES_FOLDER = {
    "NFO": r"GFDLNFO_TICK_03012025",  # ⬅️ CHANGE THIS
    "BFO": r"BFO_TICK_03012025"       # ⬅️ CHANGE THIS
}
```

### Examples:

**Single date folder:**
```python
GDFL_FILES_FOLDER = {
    "NFO": r"GFDLNFO_TICK_03012025",
    "BFO": r"BFO_TICK_03012025"
}
```

**Yearly folder:**
```python
GDFL_FILES_FOLDER = {
    "NFO": r"GFDLNFO_2025",
    "BFO": r"BFO_2025"
}
```

**Full path (Windows):**
```python
GDFL_FILES_FOLDER = {
    "NFO": r"D:\Trading_Data\NFO_2025",
    "BFO": r"D:\Trading_Data\BFO_2025"
}
```

**Full path (Linux/Mac):**
```python
GDFL_FILES_FOLDER = {
    "NFO": "/home/user/trading_data/NFO_2025",
    "BFO": "/home/user/trading_data/BFO_2025"
}
```

Then run:
```bash
python processToParquet.py
```

---

## Method 2: Command Line Arguments (Quick One-Time Use)

Specify folders when running the script:

```bash
python processToParquet.py [NFO_FOLDER] [BFO_FOLDER] [OUTPUT_FOLDER]
```

### Examples:

**Specify only NFO folder:**
```bash
python processToParquet.py GFDLNFO_2025
```

**Specify NFO and BFO folders:**
```bash
python processToParquet.py GFDLNFO_2025 BFO_2025
```

**Specify all three (NFO, BFO, and output):**
```bash
python processToParquet.py GFDLNFO_2025 BFO_2025 MyOutput
```

**With full paths:**
```bash
python processToParquet.py "D:\Data\NFO_2025" "D:\Data\BFO_2025" "D:\Output\Parquet"
```

---

## Method 3: Batch File (Windows) - Set It and Forget It

Create a batch file `process_my_data.bat`:

```batch
@echo off
echo Processing GDFL Data...
python processToParquet.py "D:\Trading_Data\NFO_2025" "D:\Trading_Data\BFO_2025" "D:\Output\Parquet"
pause
```

Double-click the batch file to run!

---

## Supported Folder Structures

### Structure 1: Single Date Folder
```
GFDLNFO_TICK_03012025/
  ├── Futures/
  │   └── -I/
  │       └── NIFTY-I.NFO.csv
  └── Options/
      └── NIFTY06FEB2524000CE.NFO.csv
```

### Structure 2: Yearly Folder (Multiple Dates)
```
GFDLNFO_2025/
  ├── Futures/
  │   ├── 01012025/
  │   │   └── -I/
  │   ├── 02012025/
  │   │   └── -I/
  │   └── 03012025/
  │       └── -I/
  └── Options/
      ├── 01012025/
      ├── 02012025/
      └── 03012025/
```

Both structures are automatically detected!

---

## Output Directories

### Default Output Locations:
- **Parquet**: `ProcessedParquet/`
- **CSV**: `ProcessedCSV/`
- **Logs**: `Logs/`

### Custom Output (Method 1 - config.py):
```python
OUTPUT_PARQUET_DIR = r"D:\MyOutput\Parquet"
OUTPUT_CSV_DIR = r"D:\MyOutput\CSV"
```

### Custom Output (Method 2 - command line):
```bash
python processToParquet.py GFDLNFO_2025 BFO_2025 "D:\MyOutput"
```

---

## Quick Start Guide

### For First Time Use:

1. **Edit config.py**:
   - Find the section: `# INPUT DIRECTORIES - CHANGE THESE`
   - Update `"NFO"` and `"BFO"` paths to your folders
   - Save the file

2. **Run the processor**:
   ```bash
   python processToParquet.py
   ```

3. **Find your output**:
   - Check `ProcessedParquet/` folder
   - Files named like: `nifty_call_03012025.parquet`

### For Yearly Data:

1. **Point to yearly folder in config.py**:
   ```python
   GDFL_FILES_FOLDER = {
       "NFO": r"GFDLNFO_2025",
       "BFO": r"BFO_2025"
   }
   ```

2. **Run once** - it processes ALL dates automatically:
   ```bash
   python processToParquet.py
   ```

3. **Output** - one file per date:
   - `nifty_call_01012025.parquet`
   - `nifty_call_02012025.parquet`
   - `nifty_call_03012025.parquet`
   - ... and so on

---

## Common Issues

### Issue: "Folder does not exist"
**Solution**: Check the path in config.py - use `r"path"` for Windows paths

### Issue: "No data found"
**Solution**: Verify your folder structure matches one of the supported formats

### Issue: Processing is slow
**Solution**: This is normal for large datasets - check `Logs/` folder for progress

---

## Need Help?

Check the log files in `Logs/` folder - they show exactly what's being processed!

