################################################################################ importing libraries
import subprocess
import logging
import shutil
import config
import sys
import os

# Command-line argument support
if len(sys.argv) > 1:
    # Override config with command-line arguments
    # Usage: python processToParquet.py [NFO_FOLDER] [BFO_FOLDER] [OUTPUT_FOLDER]
    if len(sys.argv) >= 2:
        config.GDFL_FILES_FOLDER['NFO'] = sys.argv[1]
        logging.info(f"Using command-line NFO folder: {sys.argv[1]}")
    if len(sys.argv) >= 3:
        config.GDFL_FILES_FOLDER['BFO'] = sys.argv[2]
        logging.info(f"Using command-line BFO folder: {sys.argv[2]}")
    if len(sys.argv) >= 4:
        config.OUTPUT_PARQUET_DIR = sys.argv[3]
        logging.info(f"Using command-line output folder: {sys.argv[3]}")

# Create necessary directories (don't delete existing files - allows multi-date processing)
for dirr in [config.OUTPUT_PARQUET_DIR]:
    os.makedirs(dirr, exist_ok=True)

os.makedirs(config.LOG_FILE_FOLDER, exist_ok=True)

logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(name)s] [%(threadName)s]: %(message)s', level=logging.DEBUG)

logging.info("Started processing GDFL data to Parquet.")

# Instrument types and indices to process
instutype = ["call", "put", "future"]
indexname = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]

logging.info(f"Processing data from NFO: {config.GDFL_FILES_FOLDER['NFO']}")
logging.info(f"Processing data from BFO: {config.GDFL_FILES_FOLDER['BFO']}")
logging.info(f"Output will be saved to: {config.OUTPUT_PARQUET_DIR}")

for instu in instutype:
    for indice in indexname:
        logging.info(f"Processing {indice} {instu}...")
        subprocess.run(['python', "prepareTickByTickData_Parquet.py", indice, instu])

logging.info(f"Processing complete. Parquet files saved in '{config.OUTPUT_PARQUET_DIR}' folder.")
logging.info("Stopped.")

