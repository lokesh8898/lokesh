# Processing Multiple Dates - Complete Guide

## 📁 Your Current Folder Structure

```
NFO/
  └── GFDLNFO_TICK_03012025/
      └── GFDLNFO_TICK_03012025/
          ├── Futures/
          │   ├── -I/  ← CSV files
          │   ├── -II/
          │   └── -III/
          └── Options/
              ├── -I/  ← CSV files
              ├── -II/
              └── -III/

BFO/
  └── BFO_TICK_03012025/
      └── BFO_TICK_03012025/  ← CSV files directly
```

---

## ✅ **What Happens with Multiple Dates?**

### **Example: Adding a Second Date**

If you add `GFDLNFO_TICK_04012025` and `BFO_TICK_04012025`:

```
NFO/
  ├── GFDLNFO_TICK_03012025/
  │   └── GFDLNFO_TICK_03012025/
  │       ├── Futures/...
  │       └── Options/...
  └── GFDLNFO_TICK_04012025/
      └── GFDLNFO_TICK_04012025/
          ├── Futures/...
          └── Options/...

BFO/
  ├── BFO_TICK_03012025/
  │   └── BFO_TICK_03012025/...
  └── BFO_TICK_04012025/
      └── BFO_TICK_04012025/...
```

---

## 🚀 **3 Ways to Process Multiple Dates**

### **Option 1: Automatic Batch Processing (RECOMMENDED)**

Process **ALL dates automatically** with a single command:

```bash
python processAllDates.py
```

**What it does:**
- ✅ Automatically scans `NFO/` and `BFO/` folders
- ✅ Finds all dated subfolders (handles double nesting)
- ✅ Matches dates between NFO and BFO
- ✅ Processes each date one by one
- ✅ Creates Parquet files like:
  - `nifty_call_03012025.parquet`
  - `nifty_call_04012025.parquet`
  - `sensex_put_03012025.parquet`
  - etc.

**Output:**
```
Scanning NFO parent folder: NFO
Found dated folder: NFO\GFDLNFO_TICK_03012025\GFDLNFO_TICK_03012025
Found dated folder: NFO\GFDLNFO_TICK_04012025\GFDLNFO_TICK_04012025
Found 2 NFO date folder(s)

Scanning BFO parent folder: BFO
Found dated folder: BFO\BFO_TICK_03012025\BFO_TICK_03012025
Found dated folder: BFO\BFO_TICK_04012025\BFO_TICK_04012025
Found 2 BFO date folder(s)

Common dates to process: ['03012025', '04012025']

[1/2] Processing date: 03012025
  NFO: NFO\GFDLNFO_TICK_03012025\GFDLNFO_TICK_03012025
  BFO: BFO\BFO_TICK_03012025\BFO_TICK_03012025
... processing ...

[2/2] Processing date: 04012025
  NFO: NFO\GFDLNFO_TICK_04012025\GFDLNFO_TICK_04012025
  BFO: BFO\BFO_TICK_04012025\BFO_TICK_04012025
... processing ...

Processed 2 date(s)
```

---

### **Option 2: Process Single Date (Manual Control)**

Update `config.py` to point to a specific date:

```python
GDFL_FILES_FOLDER = {
    "NFO": r"NFO\GFDLNFO_TICK_04012025\GFDLNFO_TICK_04012025",  # ⬅️ Change date here
    "BFO": r"BFO\BFO_TICK_04012025\BFO_TICK_04012025"           # ⬅️ Change date here
}
```

Then run:
```bash
python processToParquet.py
```

**When to use:**
- You want precise control over which date to process
- You're testing with a specific date
- You only have one date to process

---

### **Option 3: Command-Line Arguments**

Process a specific date without changing `config.py`:

```bash
python processToParquet.py "NFO\GFDLNFO_TICK_04012025\GFDLNFO_TICK_04012025" "BFO\BFO_TICK_04012025\BFO_TICK_04012025"
```

**When to use:**
- Quick one-time processing
- Scripting/automation from batch files
- Different dates for different runs

---

## 📊 **Output Files**

All Parquet files are saved in `ProcessedParquet/` with naming format:

```
{index}_{instrument_type}_{date}.parquet
```

**Examples:**
- `nifty_call_03012025.parquet`
- `nifty_put_03012025.parquet`
- `nifty_future_03012025.parquet`
- `banknifty_call_03012025.parquet`
- `sensex_call_03012025.parquet`
- etc.

For **multiple dates**, you'll have separate files for each date:
- `nifty_call_03012025.parquet`
- `nifty_call_04012025.parquet`
- `nifty_call_05012025.parquet`

---

## 🔧 **How to Configure for Your Setup**

### **Edit `processAllDates.py` (lines 71-72):**

```python
NFO_PARENT = r"NFO"  # ⬅️ Parent folder with all NFO dates
BFO_PARENT = r"BFO"  # ⬅️ Parent folder with all BFO dates
```

**If your data is in a different location:**
```python
NFO_PARENT = r"D:\TradingData\NFO_2025"
BFO_PARENT = r"D:\TradingData\BFO_2025"
```

---

## ⚙️ **Important Notes**

1. **Date Matching:**
   - The script only processes dates that exist in **BOTH** NFO and BFO
   - If you have `03012025` in NFO but not in BFO, it will be skipped
   - You'll see a warning in the logs

2. **File Structure Requirements:**
   - NFO must have `Futures/` and/or `Options/` subfolders
   - BFO can have CSV files directly or in subfolders
   - Double nesting is automatically detected

3. **Output Files:**
   - Each date gets separate Parquet files
   - Files from different dates **don't overwrite** each other
   - Date is included in the filename

4. **Performance:**
   - Processing multiple dates takes longer (proportional to data volume)
   - Each date processes independently
   - You can monitor progress in the console output

---

## 🐛 **Troubleshooting**

### **Problem: "No common dates found"**

**Cause:** Date folders don't exist in both NFO and BFO

**Solution:**
- Check folder names match: `GFDLNFO_TICK_03012025` vs `BFO_TICK_03012025`
- Verify both contain the same date (last 8 digits)
- Check logs for which dates were found in each folder

### **Problem: "Found 0 NFO/BFO date folders"**

**Cause:** The script can't find your date folders

**Solution:**
- Verify `NFO_PARENT` and `BFO_PARENT` paths in `processAllDates.py`
- Check folder structure matches the expected format
- Ensure folders contain CSV files or Futures/Options subfolders

### **Problem: Processing is very slow**

**Cause:** Large volume of tick data

**Solution:**
- This is expected for tick-by-tick data
- The optimized Parquet write (collect-then-write-once) helps
- Consider processing during off-hours
- Monitor logs to see progress per file

---

## 📝 **Quick Reference**

| Task | Command |
|------|---------|
| Process ALL dates automatically | `python processAllDates.py` |
| Process single date (from config) | `python processToParquet.py` |
| Process specific date (command-line) | `python processToParquet.py "NFO\..." "BFO\..."` |
| View output files | `dir ProcessedParquet` |
| Check logs | `dir Logs` (sorted by time) |

---

## ✅ **Summary**

Your setup now supports:
- ✅ Single date processing
- ✅ Multiple date batch processing
- ✅ Automatic date detection
- ✅ NFO (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY)
- ✅ BFO (SENSEX)
- ✅ Preserved date/time formats (DD/MM/YYYY, HH:MM:SS)
- ✅ Efficient Parquet output with compression
- ✅ Handles double-nested folder structures
- ✅ Handles segment folders (-I, -II, -III)

**Just drop new dated folders into NFO/ and BFO/, then run `python processAllDates.py`!** 🚀

