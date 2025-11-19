#################################################################################### importing libraries
from datetime import datetime
import pandas as pd
import numpy as np
import traceback
import logging
import config
import sys
import os
import re

pd.options.mode.chained_assignment = None

LOG_FILE_PATH = os.path.join(config.LOG_FILE_FOLDER, datetime.now().strftime("%d%m%Y, %H%M%S.%f")+".log")
logging.basicConfig(filename=LOG_FILE_PATH, format='%(asctime)s [%(levelname)s] [%(name)s] [%(threadName)s]: %(message)s', level=logging.DEBUG)

def handle_exception(exc_type, exc_value, exc_traceback) -> None:

    logging.info(f"An uncaught exception occurred: {exc_type}, {exc_value}, {exc_traceback}")

    if exc_traceback:
        format_exception = traceback.format_tb(exc_traceback)
        for line in format_exception:
            logging.info(f"{repr(line)}")
    
    sys.exit()

sys.excepthook = handle_exception

def getDateFolder(exchange: str) -> dict:
    """
    Discover date folders from GDFL directory.
    Supports two structures:
    1. Single date: GFDLNFO_TICK_03012025/ (date in folder name)
    2. Yearly: GFDLNFO_2025/Futures/03012025/ and GFDLNFO_2025/Options/03012025/
    """
    toreturn = {}
    
    # Get the appropriate folder based on exchange
    gdfl_folder = config.GDFL_FILES_FOLDER.get(exchange)
    if not gdfl_folder:
        logging.error(f"No folder configured for exchange: {exchange}")
        return toreturn
    
    if not os.path.exists(gdfl_folder):
        logging.error(f"Folder does not exist: {gdfl_folder}")
        return toreturn

    # Check if this is a yearly structure with Futures/Options subfolders
    has_futures_subfolder = os.path.exists(os.path.join(gdfl_folder, "Futures"))
    has_options_subfolder = os.path.exists(os.path.join(gdfl_folder, "Options"))
    
    if has_futures_subfolder or has_options_subfolder:
        # Yearly structure: scan date folders inside Futures and Options
        logging.info(f"Detected yearly folder structure in {gdfl_folder}")
        
        # Scan Futures subfolder for date folders
        if has_futures_subfolder:
            futures_folder = os.path.join(gdfl_folder, "Futures")
            futures_items = os.listdir(futures_folder)
            for item in futures_items:
                item_path = os.path.join(futures_folder, item)
                if os.path.isdir(item_path) and item not in ['-I', '-II', '-III']:
                    # Check if this looks like a date folder (8 digits)
                    if len(item) == 8 and item.isdigit():
                        if item not in toreturn:
                            toreturn[item] = set()
                        toreturn[item].add(gdfl_folder)
                        logging.info(f"Found date folder in Futures: {item}")
        
        # Scan Options subfolder for date folders
        if has_options_subfolder:
            options_folder = os.path.join(gdfl_folder, "Options")
            options_items = os.listdir(options_folder)
            for item in options_items:
                item_path = os.path.join(options_folder, item)
                if os.path.isdir(item_path) and item not in ['-I', '-II', '-III']:
                    # Check if this looks like a date folder (8 digits)
                    if len(item) == 8 and item.isdigit():
                        if item not in toreturn:
                            toreturn[item] = set()
                        toreturn[item].add(gdfl_folder)
                        logging.info(f"Found date folder in Options: {item}")
        
        # If no date folders found, extract date from the parent folder name
        if not toreturn:
            folderName = os.path.basename(gdfl_folder)
            if "_" in folderName:
                parts = folderName.split("_")
                for part in parts:
                    if len(part) == 8 and part.isdigit():
                        toreturn[part] = {gdfl_folder}
                        logging.info(f"No date subfolders found, extracted date {part} from parent folder: {folderName}")
                        return toreturn
        
        logging.info(f"Found {len(toreturn)} unique dates in {gdfl_folder}")
        return toreturn
    
    # Check if the GDFL folder itself is a dated folder (e.g., GFDLNFO_TICK_03012025)
    folderName = os.path.basename(gdfl_folder)
    
    # Try to extract date from folder name (expects format: *_DDMMYYYY)
    if "_" in folderName:
        parts = folderName.split("_")
        # Look for a part that looks like a date (8 digits)
        for part in parts:
            if len(part) == 8 and part.isdigit():
                # This is likely the date
                toreturn[part] = {gdfl_folder}
                logging.info(f"Found date {part} from folder name: {folderName} for exchange {exchange}")
                return toreturn
    
    # Fallback: scan for date-named subfolders at root level
    # Example: NFO/ contains GFDLNFO_TICK_03012025/ or BFO/ contains BFO_TICK_03012025/
    tickDateWiseFolder = os.listdir(gdfl_folder)
    tickDateWiseFolder = [ii for ii in tickDateWiseFolder if os.path.isdir(os.path.join(gdfl_folder, ii))]

    for ii in tickDateWiseFolder:
        # Skip common folder names that are not dates
        if ii.lower() in ['futures', 'options', '-i', '-ii', '-iii']:
            continue
        
        # Try to extract date from folder name (format: *_DDMMYYYY or *DDMMYYYY)
        # Examples: GFDLNFO_TICK_03012025, BFO_TICK_03012025
        if "_" in ii:
            keyy = ii.split("_")[-1]
        else:
            # Try last 8 characters
            keyy = ii[-8:] if len(ii) >= 8 else ii
            
        # Verify it looks like a date (8 digits)
        if len(keyy) == 8 and keyy.isdigit():
            if keyy not in toreturn:
                toreturn[keyy] = set()
            toreturn[keyy].add(os.path.join(gdfl_folder, ii))
            logging.info(f"Found dated subfolder: {ii} -> date: {keyy}")

    return toreturn

def getFinalDatasetToPush(dataFilePath: str, optionInfo: dict, outputFilePath: str, isFutureDataFile: bool) -> None:

    logging.info(f"Started processing: {dataFilePath}")

    try:

        optDataset = pd.read_csv(dataFilePath)
        optDataset.columns = [ii.strip().replace("<", "").replace(">", "").lower() for ii in optDataset.columns]
        optDataset = optDataset.rename(columns={
            "o/i": "oi", "ticker": "symbol", "open interest": "oi", "higgh": "high", "dtae": "date", "openinterest": "oi", "opne interest": "oi", 
            "open inerest": "oi", "closeq": "close", "ltq": "volume"
        })

        optDataset = optDataset[(~pd.isnull(optDataset['symbol'])) & (optDataset['volume'] != 0)]
        optDataset = optDataset.drop_duplicates(subset=['time'], keep="last")
        optDataset['symbol'] = optionInfo['symbolName']

        for columnss in ['date', 'time']:
            optDataset[columnss] = optDataset[columnss].str.strip()

        # Keep original date and time formats - no conversion
        # Parse for filtering only
        time_parsed = pd.to_datetime(optDataset["time"], format="%H:%M:%S")
        
        try:
            date_parsed = pd.to_datetime(optDataset['date'], format="%d/%m/%Y")
        except Exception:
            date_parsed = pd.to_datetime(optDataset['date'], format="%d-%m-%Y")
        
        if not isFutureDataFile:
            optDataset['expiry'] = optionInfo['expiry']
            optDataset['strike'] = optionInfo['strike']
        
        # Use default exchange if not provided (NFO for NSE indices)
        exchange = optionInfo.get('exchange', 'NFO')
        optDataset['toKeep'] = time_parsed.apply(lambda x: config.MARKET_TIMINGS[exchange]['start'] <= x.time() <= config.MARKET_TIMINGS[exchange]['end'])
        optDataset = optDataset[optDataset['toKeep']]
        if optDataset.empty:
            return

        optDataset = optDataset.drop(columns=['toKeep'])
        optDataset['open'] = optDataset['high'] = optDataset['low'] = optDataset['close'] = optDataset['ltp']
        
        # Keep date and time as original strings (no conversion to integers)
        # Date stays as DD/MM/YYYY or DD-MM-YYYY
        # Time stays as HH:MM:SS

        # Convert prices to integers (multiply by 100) but keep volume, OI as-is
        for priceCol in ["open", "high", "low", "close"]:
            if priceCol not in optDataset.columns:
                continue
            optDataset[priceCol] *= 100
            optDataset[priceCol] = optDataset[priceCol].astype(np.int32)

        optDataset['symbol'] = optDataset['symbol'].astype('category')

        if isFutureDataFile:
            optDataset = optDataset[['date', 'time', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'oi']]
        else:
            optDataset = optDataset[['date', 'time', 'symbol', 'strike', 'expiry', 'open', 'high', 'low', 'close', 'volume', 'oi']]

        if os.path.exists(outputFilePath):
            optDataset.to_csv(outputFilePath, mode='a', index=False, header=False)
        else:
            optDataset.to_csv(outputFilePath, index=False)
            
        logging.info(f"Successfully processed and saved: {outputFilePath}")
        return
    
    except Exception:
        logging.error(f"Error processing {dataFilePath}: {traceback.format_exc()}")

    return


if __name__ == "__main__":
    
    logging.info("Utility started.")

    if len(sys.argv) != 3:
        logging.info("Command line parameters are not passed properly. Usage: python prepareTickByTickData_CSV.py <INDEX> <instrument_type>")
        exit()

    uPara = {"index": sys.argv[1].upper(), "instrument_type": sys.argv[2].lower()}
    
    # Default exchange mapping (can be customized)
    EXCHANGE_MAPPING = {
        "NIFTY": "NFO",
        "BANKNIFTY": "NFO",
        "FINNIFTY": "NFO",
        "MIDCPNIFTY": "NFO",
        "SENSEX": "BFO"
    }
    
    # Get the exchange for this index
    exchange = EXCHANGE_MAPPING.get(uPara['index'], 'NFO')
    logging.info(f"Processing {uPara['index']} from {exchange} exchange")
    
    DATE_FOLDERS = getDateFolder(exchange)
    logging.info(f"Got following folder info {DATE_FOLDERS} for processing {uPara}.")

    for tradingdatekey in DATE_FOLDERS:     

        # Keep trading date in original DD/MM/YYYY format
        tradingdate_obj = datetime.strptime(tradingdatekey, '%d%m%Y')
        tradingdate_str = tradingdate_obj.strftime('%d/%m/%Y')
        
        for folderpath in DATE_FOLDERS[tradingdatekey]:

            containssubfolder = ("Futures" in os.listdir(folderpath)) or ("Options" in os.listdir(folderpath))

            logging.info(f"Started processing folder: {folderpath} for {uPara}, date: {tradingdatekey}")

            if uPara['instrument_type'] == "future":

                # Handle different folder structures for NFO vs BFO
                if exchange == "BFO":
                    # BFO structure: check for date subfolder or files directly in root
                    if containssubfolder:
                        # Check if there's a date-specific folder
                        date_folder_path = os.path.join(folderpath, "Futures", tradingdatekey)
                        if os.path.exists(date_folder_path):
                            folderpath = date_folder_path
                            logging.info(f"Using date-specific Futures folder: {date_folder_path}")
                    
                    existingfilesnamesinfolder = os.listdir(folderpath)
                    # Look for futures files: SENSEX28JAN25FUT.BFO.csv
                    futurefilepath = [ii for ii in existingfilesnamesinfolder 
                                     if ii.startswith(f"{uPara['index'].upper()}") and "FUT" in ii and ii.endswith('.csv')]
                else:
                    # NFO has subfolder structure
                    if containssubfolder:
                        # Check for yearly structure: Futures/DDMMYYYY/ or single date: Futures/-I/
                        futures_base = os.path.join(folderpath, "Futures")
                        date_folder_path = os.path.join(futures_base, tradingdatekey)
                        
                        if os.path.exists(date_folder_path):
                            # Yearly structure with date folders
                            folderpath = os.path.join(date_folder_path, "-I")
                            logging.info(f"Using yearly Futures structure: {folderpath}")
                        else:
                            # Single date structure
                            folderpath = os.path.join(futures_base, "-I")
                    
                    existingfilesnamesinfolder = os.listdir(folderpath)
                    # Look for NFO futures: NIFTY-I.NFO.csv
                    futurefilepath = [ii for ii in existingfilesnamesinfolder if ii.startswith(f"{uPara['index'].upper()}-I.")]

                if len(futurefilepath) >= 1:
                    outputFilePath = os.path.join(config.OUTPUT_CSV_DIR, f"{uPara['index'].lower()}_future_{tradingdatekey}.csv")
                    
                    # Process all future files (BFO may have multiple expiries)
                    for future_file in futurefilepath:
                        getFinalDatasetToPush(dataFilePath=os.path.join(folderpath, future_file), optionInfo={
                            "underlyingName": uPara['index'], 
                            "symbolName": uPara['index'], 
                            "exchange": exchange
                        }, outputFilePath=outputFilePath, isFutureDataFile=True)
                else:
                    logging.info(f"{uPara['index']}, Unable to find future file. Hence, can't process data.")
                    
            else:

                # Handle different folder structures for NFO vs BFO
                if exchange == "BFO":
                    # BFO structure: check for date subfolder or files directly in root
                    if containssubfolder:
                        # Check if there's a date-specific folder
                        date_folder_path = os.path.join(folderpath, "Options", tradingdatekey)
                        if os.path.exists(date_folder_path):
                            folderpath = date_folder_path
                            logging.info(f"Using date-specific Options folder: {date_folder_path}")
                    
                    existingfilesnamesinfolder = os.listdir(folderpath)
                else:
                    # NFO has subfolder structure
                    if containssubfolder:
                        # Check for yearly structure: Options/DDMMYYYY/ or single date: Options/
                        options_base = os.path.join(folderpath, "Options")
                        date_folder_path = os.path.join(options_base, tradingdatekey)
                        
                        if os.path.exists(date_folder_path):
                            # Yearly structure with date folders
                            folderpath = date_folder_path
                            logging.info(f"Using yearly Options structure: {folderpath}")
                        else:
                            # Single date structure
                            folderpath = options_base
                    
                    existingfilesnamesinfolder = os.listdir(folderpath)
            
                # Extract symbol info from filenames
                # NFO format: NIFTY06FEB2524000CE.NFO.csv
                # BFO format: SENSEX28JAN2586100CE.BFO.csv
                symbolFiles = []
                optionType = "CE" if uPara['instrument_type'] == "call" else "PE"
                
                for filename in existingfilesnamesinfolder:
                    if filename.endswith('.csv') and filename.startswith(uPara['index'].upper()):
                        # Check if it's the correct option type (CE for call, PE for put)
                        # Remove .csv, .NFO, .BFO extensions to check the last 2 characters
                        nameWithoutExt = filename.replace('.NFO.csv', '').replace('.BFO.csv', '').replace('.csv', '')
                        if nameWithoutExt.endswith(optionType):
                            symbolFiles.append(filename)

                logging.info(f"Found {len(symbolFiles)} {uPara['instrument_type']} option files to process for {uPara['index']}.")

                outputFilePath = os.path.join(config.OUTPUT_CSV_DIR, f"{uPara['index'].lower()}_{uPara['instrument_type']}_{tradingdatekey}.csv")

                for symbolFile in symbolFiles:
                    try:
                        # Parse symbol name to extract strike and expiry
                        # Remove file extensions
                        symbolName = symbolFile.replace('.NFO.csv', '').replace('.BFO.csv', '').replace('.csv', '')
                        
                        # Extract strike and expiry from filename
                        # Format: NIFTY06FEB2524000CE -> Strike is 24000
                        # Pattern: [INDEX][DDMMMYY][STRIKE][CE/PE]
                        # Where DD=day, MMM=month (3 letters), YY=year
                        
                        # Match pattern: After the 3-letter month and 2-digit year, capture digits before CE/PE
                        # This will skip the date portion (DDMMMYY) and capture only the strike
                        match = re.search(r'[A-Z]{3}\d{2}(\d+)(?:CE|PE)$', symbolName)
                        if match:
                            strike = float(match.group(1))
                        else:
                            # Fallback: try to extract strike as last digits before CE/PE (may be wrong)
                            fallback_match = re.search(r'(\d+)(?:CE|PE)$', symbolName)
                            if fallback_match:
                                strike = float(fallback_match.group(1))
                            else:
                                strike = 0.0
                            logging.warning(f"Could not properly extract strike from {symbolName}, using {strike}")
                        
                        # For expiry, we'll use the trading date in DD/MM/YYYY format
                        # A more sophisticated parser would extract the date from the filename
                        expiry = tradingdate_str
                        
                        getFinalDatasetToPush(
                            dataFilePath=os.path.join(folderpath, symbolFile), 
                            optionInfo={
                                "underlyingName": uPara['index'], 
                                "symbolName": symbolName, 
                                "expiry": expiry, 
                                "strike": strike, 
                                "exchange": exchange
                            }, 
                            outputFilePath=outputFilePath, 
                            isFutureDataFile=False
                        )
                    except Exception as e:
                        logging.error(f"Error processing {symbolFile}: {str(e)}\n{traceback.format_exc()}")

    logging.info("Utility stopped.")

