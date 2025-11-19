#################################################################################### importing libraries
from datetime import datetime
import mysql.connector
import pandas as pd
import numpy as np
import traceback
import logging
import config
import sys
import os

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

def getDateFolder() -> dict:

    toreturn = {}

    tickDateWiseFolder = os.listdir(config.GDFL_FILES_FOLDER[0])
    tickDateWiseFolder = [ii for ii in tickDateWiseFolder if os.path.isdir(os.path.join(config.GDFL_FILES_FOLDER[0], ii))]

    for ii in tickDateWiseFolder:

        keyy = ii.split("_")[-1]
        if keyy not in toreturn:
            toreturn[keyy] = set()
        
        toreturn[keyy].add(os.path.join(config.GDFL_FILES_FOLDER[0], ii))

    return toreturn

def getFinalDatasetToPush(dataFilePath: str, optionInfo: dict, tableName: str, isFutureDataFile: bool) -> None:

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

        optDataset["time"] = pd.to_datetime(optDataset["time"], format="%H:%M:%S")
        optDataset["time"] = optDataset["time"].apply(lambda x: datetime(config.TODAY.year, config.TODAY.month, config.TODAY.day, x.hour, x.minute, x.second))
        
        try:
            optDataset['date'] = pd.to_datetime(optDataset['date'], format="%d/%m/%Y")
        except Exception:
            optDataset['date'] = pd.to_datetime(optDataset['date'], format="%d-%m-%Y")
        
        if not isFutureDataFile:
            optDataset['expiry'] = optionInfo['expiry']
            optDataset['strike'] = optionInfo['strike']
        
        optDataset['toKeep'] = optDataset.apply(lambda x: config.MARKET_TIMINGS[optionInfo['exchange']]['start'] <= x['time'].time() <= config.MARKET_TIMINGS[optionInfo['exchange']]['end'], axis=1)
        optDataset = optDataset[optDataset['toKeep']]
        if optDataset.empty:
            return

        optDataset = optDataset.drop(columns=['toKeep'])
        optDataset["time"] = optDataset["time"] - datetime(config.TODAY.year, config.TODAY.month, config.TODAY.day, 0)
        optDataset["time"] = optDataset["time"].dt.total_seconds()
        optDataset['open'] = optDataset['high'] = optDataset['low'] = optDataset['close'] = optDataset['ltp']
        
        optDataset["date"] = optDataset["date"].dt.strftime("%y%m%d")

        for int32Col in ["time", "date", "oi", "volume", "expiry"]:
            if int32Col not in optDataset.columns:
                continue
            optDataset[int32Col] = optDataset[int32Col].astype(np.int32)

        for int32Col in ["open", "high", "low", "close"]:
            if int32Col not in optDataset.columns:
                continue
            optDataset[int32Col] *= 100
            optDataset[int32Col] = optDataset[int32Col].astype(np.int32)

        optDataset['symbol'] = optDataset['symbol'].astype('category')

        if isFutureDataFile:
            optDataset = optDataset[['date', 'time', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'oi']]
        else:
            optDataset = optDataset[['date', 'time', 'symbol', 'strike', 'expiry', 'open', 'high', 'low', 'close', 'volume', 'oi']]

        filePath = os.path.join(config.UPLOAD_DIR, f"{tableName}.csv")
        if os.path.exists(filePath):
            optDataset.to_csv(filePath, mode='a', index=False, header=False)
        else:
            optDataset.to_csv(filePath, index=False)
            
        return
    
    except Exception:
        logging.error(traceback.format_exc())

    return


if __name__ == "__main__":
    
    logging.info("Utility started.")

    DATE_FOLDERS = getDateFolder()
    if len(sys.argv) != 3:
        logging.info("Command line parameters are not passed properly.")
        exit()

    uPara = {"index": sys.argv[1].upper(), "instrument_type": sys.argv[2].lower()}

    logging.info(f"Got following folder info {DATE_FOLDERS} for DB updation for {uPara}.")
    
    tickDbObj = mysql.connector.connect(host=config.MYSQL_HOST, user=config.MYSQL_USER, password=config.MYSQL_PASSWORD, database="tickhistoricaldb")
    tickCursorObj = tickDbObj.cursor()

    minuteDbObj = mysql.connector.connect(host=config.MYSQL_MINUTE_HOST, user=config.MYSQL_MINUTE_USER, password=config.MYSQL_MINUTE_PASSWORD, database="historicaldb")
    minuteCursorObj = minuteDbObj.cursor()

    minuteCursorObj.execute("select scrip, exchange from metadata")
    INSTRUMENT_INFO = {ii[0].upper(): {"exchange": ii[1].upper()} for ii in minuteCursorObj.fetchall()}

    for tradingdatekey in DATE_FOLDERS:     

        tradingdate = int(datetime.strptime(tradingdatekey, '%d%m%Y').strftime('%y%m%d'))
        for folderpath in DATE_FOLDERS[tradingdatekey]:

            containssubfolder = ("Futures" in os.listdir(folderpath)) or ("Options" in os.listdir(folderpath))

            logging.info(f"Started processing folder: {folderpath} for {uPara}")

            if uPara['instrument_type'] == "future":

                if containssubfolder:
                    folderpath = os.path.join(os.path.join(folderpath, "Futures"), "-I")
                
                existingfilesnamesinfolder = os.listdir(folderpath)

                tickCursorObj.execute(f"select distinct(symbol) from {uPara['index'].lower()}_future where date = {tradingdate}")
                existingSymbols = [record[0] for record in tickCursorObj.fetchall()]

                if existingSymbols:
                    logging.info(f"{uPara['index']}, future data already exists for {tradingdatekey}.")
                    continue
            
                futurefilepath = [ii for ii in existingfilesnamesinfolder if ii.startswith(f"{uPara['index'].upper()}-I.")]

                if len(futurefilepath) == 1:

                    getFinalDatasetToPush(dataFilePath=os.path.join(folderpath, futurefilepath[0]), optionInfo={
                        "underlyingName": uPara['index'], "symbolName": uPara['index'], "exchange": INSTRUMENT_INFO[uPara['index']]['exchange']
                    }, tableName=f"{uPara['index'].lower()}_future", isFutureDataFile=True)

                else:
                    logging.info(f"{uPara['index']}, Unable to find future file. Hence, cann't upload data.")
                    
            else:

                if containssubfolder:
                    folderpath = os.path.join(folderpath, "Options")

                existingfilesnamesinfolder = os.listdir(folderpath)
            
                tableName = f"{uPara['index'].lower()}_{uPara['instrument_type']}"

                tickCursorObj.execute(f"select distinct(symbol) from {tableName} where date = {tradingdate}")
                existingSymbols = [record[0] for record in tickCursorObj.fetchall()]

                minuteCursorObj.execute(f"select distinct(symbol), strike, expiry from {tableName} where date = {tradingdate}")
                symbolToFetchDataFor = {infoo[0]: {"strike": infoo[1], "expiry": infoo[2]} for infoo in minuteCursorObj.fetchall() if infoo[0] not in existingSymbols}

                if not symbolToFetchDataFor:
                    logging.info(f"{uPara['index']}, {uPara['instrument_type']} data already exists for {tradingdatekey}.")
                    continue
            
                symbolsForWhichGDFLfilesExists = pd.DataFrame(existingfilesnamesinfolder, columns=['orifilename'])
                symbolsForWhichGDFLfilesExists['symbol'] = symbolsForWhichGDFLfilesExists['orifilename'].apply(lambda x: x.split(".")[0])
                symbolsForWhichGDFLfilesExists = symbolsForWhichGDFLfilesExists[symbolsForWhichGDFLfilesExists['symbol'].isin(symbolToFetchDataFor)]
                symbolsForWhichGDFLfilesExists = symbolsForWhichGDFLfilesExists.set_index("symbol")
                symbolsForWhichGDFLfilesExists = symbolsForWhichGDFLfilesExists.to_dict()['orifilename']
                symbolsForWhichGDFLfilesExists = {symbolName: os.path.join(folderpath, symbolsForWhichGDFLfilesExists[symbolName]) for symbolName in symbolsForWhichGDFLfilesExists}

                for symbolName in symbolsForWhichGDFLfilesExists:
                    getFinalDatasetToPush(dataFilePath=symbolsForWhichGDFLfilesExists[symbolName], optionInfo={
                        "underlyingName": uPara['index'], "symbolName": symbolName, "expiry": symbolToFetchDataFor[symbolName]['expiry'], 
                        "strike": symbolToFetchDataFor[symbolName]['strike'], "exchange": INSTRUMENT_INFO[uPara['index']]['exchange']
                    }, tableName=tableName, isFutureDataFile=False)

    logging.info("Utility stopped.")
