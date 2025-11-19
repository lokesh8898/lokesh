################################################################################ importing libraries
import subprocess
import logging
import shutil
import config
import os

for dirr in [config.UPLOAD_DIR]:

    if os.path.exists(dirr):
        shutil.rmtree(dirr)

    os.makedirs(dirr)

os.makedirs(config.LOG_FILE_FOLDER, exist_ok=True)

logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(name)s] [%(threadName)s]: %(message)s', level=logging.DEBUG)

logging.info("Started.")

logging.info("Started option data extractor.")

instutype = ["call", "put", "future"]
indexname = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]

for instu in instutype:
    for indice in indexname:
        subprocess.run(['python', "prepareTickByTickData.py", indice, instu])

logging.info("Started pushing option data to tick db.")
subprocess.run(['python', "uploadTickDataToDB.py"])

logging.info("Stopped.")
