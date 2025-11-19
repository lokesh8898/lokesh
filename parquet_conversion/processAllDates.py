################################################################################ 
# Script to automatically process all GFDLNFO and BFO date folders
# Works with structure where both are in the same parent directory
################################################################################
import subprocess
import logging
import os
import re
import config

logging.basicConfig(format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.INFO)

def extract_date_from_foldername(folder_name):
    """Extract 8-digit date from folder name like GFDLNFO_TICK_03012025 or BFO_TICK_01012025"""
    parts = folder_name.split('_')
    for part in parts:
        if len(part) == 8 and part.isdigit():
            return part
    return None

def scan_data_folder(parent_folder):
    """
    Scans parent folder and categorizes subfolders into NFO (GFDLNFO) and BFO.
    
    Returns:
        nfo_dict: {date: full_path} for GFDLNFO folders
        bfo_dict: {date: full_path} for BFO folders
    
    Example structure:
    NFO/
      ├── GFDLNFO_TICK_03012025/  → nfo_dict['03012025']
      ├── BFO_TICK_01012025/       → bfo_dict['01012025']
      └── BFO_TICK_02012025/       → bfo_dict['02012025']
    """
    nfo_dict = {}
    bfo_dict = {}
    
    if not os.path.exists(parent_folder):
        logging.error(f"Parent folder does not exist: {parent_folder}")
        return nfo_dict, bfo_dict
    
    logging.info(f"Scanning folder: {parent_folder}")
    
    items = os.listdir(parent_folder)
    for item in items:
        item_path = os.path.join(parent_folder, item)
        
        # Skip files, only process directories
        if not os.path.isdir(item_path):
            continue
        
        # Extract date from folder name
        date = extract_date_from_foldername(item)
        if not date:
            logging.debug(f"Skipping folder (no date found): {item}")
            continue
        
        # Categorize by folder name prefix
        if item.startswith('GFDLNFO'):
            # Check if it has the expected structure (Futures/Options or direct CSV)
            has_futures_options = (os.path.exists(os.path.join(item_path, "Futures")) or 
                                   os.path.exists(os.path.join(item_path, "Options")))
            
            # If no Futures/Options, check for nested folder
            if not has_futures_options:
                # Check for double nesting (e.g., GFDLNFO_TICK_03012025/GFDLNFO_TICK_03012025/)
                nested_path = os.path.join(item_path, item)
                if os.path.exists(nested_path):
                    has_nested = (os.path.exists(os.path.join(nested_path, "Futures")) or 
                                  os.path.exists(os.path.join(nested_path, "Options")))
                    if has_nested:
                        item_path = nested_path
                        logging.info(f"Found NFO (double-nested): {item} → {date}")
                    else:
                        logging.info(f"Found NFO: {item} → {date}")
                else:
                    logging.info(f"Found NFO: {item} → {date}")
            else:
                logging.info(f"Found NFO: {item} → {date}")
            
            if date in nfo_dict:
                logging.warning(f"Duplicate NFO date {date}: {item} (already have {os.path.basename(nfo_dict[date])})")
            nfo_dict[date] = item_path
            
        elif item.startswith('BFO'):
            # Check for double nesting
            nested_path = os.path.join(item_path, item)
            if os.path.exists(nested_path):
                # Check if nested folder has CSV files
                try:
                    nested_files = os.listdir(nested_path)
                    if any(f.endswith('.csv') for f in nested_files):
                        item_path = nested_path
                        logging.info(f"Found BFO (double-nested): {item} → {date}")
                    else:
                        logging.info(f"Found BFO: {item} → {date}")
                except (PermissionError, OSError):
                    logging.info(f"Found BFO: {item} → {date}")
            else:
                logging.info(f"Found BFO: {item} → {date}")
            
            if date in bfo_dict:
                logging.warning(f"Duplicate BFO date {date}: {item} (already have {os.path.basename(bfo_dict[date])})")
            bfo_dict[date] = item_path
        else:
            logging.debug(f"Skipping folder (unknown type): {item}")
    
    return nfo_dict, bfo_dict

# ============================================================================
# MAIN PROCESSING
# ============================================================================

logging.info("="*70)
logging.info("BATCH PROCESSING: ALL GFDLNFO AND BFO DATE FOLDERS")
logging.info("="*70)

# Use DATA_PARENT_FOLDER from config
PARENT_FOLDER = config.DATA_PARENT_FOLDER
logging.info(f"Parent folder: {PARENT_FOLDER}")

# Scan and categorize folders
nfo_dict, bfo_dict = scan_data_folder(PARENT_FOLDER)

logging.info("\n" + "-"*70)
logging.info("DISCOVERY SUMMARY")
logging.info("-"*70)
logging.info(f"NFO folders found: {len(nfo_dict)}")
for date in sorted(nfo_dict.keys()):
    logging.info(f"  {date}: {nfo_dict[date]}")

logging.info(f"\nBFO folders found: {len(bfo_dict)}")
for date in sorted(bfo_dict.keys()):
    logging.info(f"  {date}: {bfo_dict[date]}")

# Get all unique dates
all_dates = set(list(nfo_dict.keys()) + list(bfo_dict.keys()))
logging.info(f"\nTotal unique dates: {len(all_dates)}")
logging.info(f"Dates: {sorted(all_dates)}")

if not all_dates:
    logging.error("No date folders found! Please check your folder structure.")
    exit(1)

# Find dates that exist in BOTH NFO and BFO
common_dates = set(nfo_dict.keys()) & set(bfo_dict.keys())
nfo_only_dates = set(nfo_dict.keys()) - set(bfo_dict.keys())
bfo_only_dates = set(bfo_dict.keys()) - set(nfo_dict.keys())

logging.info("\n" + "-"*70)
logging.info("DATE MATCHING")
logging.info("-"*70)
logging.info(f"Dates in BOTH NFO and BFO: {len(common_dates)}")
if common_dates:
    logging.info(f"  {sorted(common_dates)}")

if nfo_only_dates:
    logging.info(f"Dates ONLY in NFO: {len(nfo_only_dates)}")
    logging.info(f"  {sorted(nfo_only_dates)}")

if bfo_only_dates:
    logging.info(f"Dates ONLY in BFO: {len(bfo_only_dates)}")
    logging.info(f"  {sorted(bfo_only_dates)}")

# Decision: Process ALL dates
# For dates with no match, use the closest available folder or skip that exchange
if not common_dates:
    logging.warning("\n⚠️  No common dates found between NFO and BFO!")
    logging.warning("Will process each date independently using available data.")

# Strategy: Process all unique dates
# For each date, use available NFO and/or BFO folder
dates_to_process = all_dates

# ============================================================================
# PROCESS ALL MATCHING DATES
# ============================================================================

logging.info("\n" + "="*70)
logging.info("STARTING PROCESSING")
logging.info("="*70)

processed_count = 0
failed_count = 0
skipped_count = 0

for date in sorted(dates_to_process):
    processed_count += 1
    
    # Check if this date has NFO and/or BFO data
    has_nfo = date in nfo_dict
    has_bfo = date in bfo_dict
    
    nfo_folder = nfo_dict.get(date)
    bfo_folder = bfo_dict.get(date)
    
    logging.info(f"\n{'='*70}")
    logging.info(f"[{processed_count}/{len(dates_to_process)}] Processing date: {date}")
    logging.info(f"{'='*70}")
    
    # If date is missing from one exchange, use a placeholder or skip
    if not has_nfo and not has_bfo:
        logging.warning(f"⚠️  Skipping date {date} - no data found")
        skipped_count += 1
        continue
    
    if not has_nfo:
        logging.warning(f"⚠️  NFO data not available for {date}")
        logging.warning(f"    Will process BFO only (SENSEX)")
        # Use a dummy placeholder for NFO (processToParquet will skip NFO indices)
        nfo_folder = bfo_folder  # Use BFO folder as placeholder
        
    if not has_bfo:
        logging.warning(f"⚠️  BFO data not available for {date}")
        logging.warning(f"    Will process NFO only (NIFTY, BANKNIFTY, etc.)")
        # Use a dummy placeholder for BFO (processToParquet will skip SENSEX)
        bfo_folder = nfo_folder  # Use NFO folder as placeholder
    
    logging.info(f"NFO: {nfo_folder}")
    logging.info(f"BFO: {bfo_folder}")
    logging.info(f"Output: {config.OUTPUT_PARQUET_DIR}")
    
    # Run processToParquet.py with command-line arguments
    result = subprocess.run([
        'python', 
        'processToParquet.py',
        nfo_folder,  # NFO folder path
        bfo_folder,  # BFO folder path
        config.OUTPUT_PARQUET_DIR  # Output directory
    ])
    
    if result.returncode != 0:
        logging.error(f"❌ Processing failed for date {date}")
        failed_count += 1
    else:
        logging.info(f"✅ Successfully processed date {date}")

# ============================================================================
# FINAL SUMMARY
# ============================================================================

logging.info("\n" + "="*70)
logging.info("BATCH PROCESSING COMPLETE!")
logging.info("="*70)
logging.info(f"Total dates found: {len(dates_to_process)}")
logging.info(f"Successfully processed: {processed_count - failed_count - skipped_count}")
logging.info(f"Failed: {failed_count}")
logging.info(f"Skipped: {skipped_count}")
logging.info(f"\nOutput location: {config.OUTPUT_PARQUET_DIR}")
logging.info(f"Logs location: {config.LOG_FILE_FOLDER}")
logging.info("="*70)
