################################################################################ importing libraries
from datetime import datetime, time

MISSING_DTS_DIR = "Missing Dates"
LOG_FILE_FOLDER = "Logs"
UPLOAD_DIR = "DataToUpload"
OUTPUT_CSV_DIR = "ProcessedCSV"  # Output directory for processed CSV files
OUTPUT_PARQUET_DIR = "ProcessedParquet"  # Output directory for processed Parquet files

# ============================================================================
# INPUT DIRECTORIES - CHANGE THESE TO YOUR DATA FOLDERS
# ============================================================================
# These can be:
#   1. Single-date folders: GFDLNFO_TICK_03012025/Futures/Options
#   2. Yearly folders: GFDLNFO_2025/Futures/DDMMYYYY/ and Options/DDMMYYYY/
#
# Examples:
#   "NFO": r"GFDLNFO_TICK_03012025"  (single date)
#   "NFO": r"GFDLNFO_2025"           (yearly with date subfolders)
#   "NFO": r"D:\Trading_Data\NFO"    (full path)
# ============================================================================

# Parent directory containing all GFDLNFO_TICK_* and BFO_TICK_* folders
DATA_PARENT_FOLDER = r"NFO"  # ⬅️ CHANGE THIS: Directory containing all date folders

# These are kept for backward compatibility (not used by processAllDates.py)
GDFL_FILES_FOLDER = {
    "NFO": r"NFO\GFDLNFO_TICK_03012025",  # For single-date processing
    "BFO": r"NFO\BFO_TICK_03012025"       # For single-date processing
}

# For folders without date in name, specify the date manually (DDMMYYYY format)
# DEFAULT_TRADING_DATE = "03012025"  # Used when folder name doesn't contain date

# ============================================================================
# OUTPUT DIRECTORIES - CHANGE THESE IF NEEDED
# ============================================================================
# You can also specify custom output paths:
# OUTPUT_CSV_DIR = r"D:\Output\CSV"
# OUTPUT_PARQUET_DIR = r"D:\Output\Parquet"
# ============================================================================
SPECIAL_CHARACTERS = [
    "-", "&"
]

TODAY = datetime.now()

MYSQL_PASSWORD = "master76"
MYSQL_HOST = "localhost"
MYSQL_USER = "root"

MYSQL_MINUTE_PASSWORD = "master76"
MYSQL_MINUTE_HOST = "192.168.173.180"
MYSQL_MINUTE_USER = "ajay"

# MYSQL_MINUTE_PASSWORD = "master76"
# MYSQL_MINUTE_HOST = "106.51.63.60"
# MYSQL_MINUTE_USER = "ajay"

MARKET_TIMINGS = {
    "NFO": {
        "start": time(9, 15), "end": time(15, 30)
    },
    "BFO": {
        "start": time(9, 15), "end": time(15, 30)
    }
}
VALID_MONTHS = [
    'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'
]
WORDS_TO_RM = [
    ".NFO", ".BFO", ".MCX"
]
TO_AVOID = [
    'NIFTYIT', 'NIFTYMID50', 'NIFTYCPSE', "SENSEX50", "GOLDGUINEA", "GOLDPETAL", "SILVERMIC", "ZINCMINI"
]
SYMBOL_CHANGE = {
    "SRTRANSFIN": "SHRIRAMFIN", "MCDOWELL_N": "UNITDSPR", "MOTHERSUMI": "MOTHERSON", "L_TFH": "LTF", "CADILAHC": "ZYDUSLIFE", "PVR": "PVRINOX", "LTI": "LTIM"
}