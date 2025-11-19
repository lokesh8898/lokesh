#################################################################################### importing libraries
from datetime import datetime
import mysql.connector
import traceback
import logging
import config
import os

LOG_FILE_PATH = os.path.join(config.LOG_FILE_FOLDER, datetime.now().strftime("%d%m%Y, %H%M%S.%f")+".log")
logging.basicConfig(filename=LOG_FILE_PATH, format='%(asctime)s [%(levelname)s] [%(name)s] [%(threadName)s]: %(message)s', level=logging.DEBUG)


if __name__ == "__main__":

    DELIMITER = (",", '"', "\n")
    SQL_QUERY = "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY '{}' ENCLOSED BY '{}' LINES TERMINATED BY '{}' IGNORE 1 LINES;"

    logging.info("Started")

    try:

        if not os.path.exists(config.UPLOAD_DIR):
            logging.info("Tick data directory not found.")
            exit()

        tableNames = os.listdir(config.UPLOAD_DIR)
        tableNames = [ii.split(".")[0] for ii in tableNames if ii.endswith(".csv")]

        mydb = mysql.connector.connect(host=config.MYSQL_HOST, user=config.MYSQL_USER, password=config.MYSQL_PASSWORD, allow_local_infile=True)
        
        mycursor = mydb.cursor()
        mycursor.execute("USE tickhistoricaldb")
        mycursor.execute("SET GLOBAL local_infile=1")

        mycursor.execute("SELECT table_name FROM information_schema.tables;")
        existingTables = [ii[0] for ii in mycursor.fetchall()]

        for tableN in tableNames:

            logging.info(f"Going to push data for {tableN}")

            filepath = os.path.join(config.UPLOAD_DIR, f"{tableN}.csv")
            filepath = os.path.join(os.getcwd(), filepath)

            if tableN not in existingTables:

                logging.info(f"Following table: {tableN} doesn't exists, creating table.")
                
                isfuturetable = tableN.endswith("_future")
                isoptiontable = tableN.endswith("_call") or tableN.endswith("_put")

                if (not isfuturetable) and (not isoptiontable):
                    continue

                if isfuturetable:
                    tablecreationquery = f"CREATE TABLE IF NOT EXISTS {tableN} (date INT, time INT, symbol TEXT, open INT, high INT, low INT, close INT, volume INT, oi INT);"
                elif isoptiontable:
                    tablecreationquery = f"CREATE TABLE IF NOT EXISTS {tableN} (date INT, time INT, symbol TEXT, strike FLOAT, expiry INT, open INT, high INT, low INT, close INT, volume INT, oi INT);"
                
                mycursor.execute(tablecreationquery)
                logging.info(f"Created table: {tableN}")

            filepath = os.path.abspath(filepath).replace("\\", "/")

            logging.info(f"Pushing data for {tableN} from {filepath}.")
            mycursor.execute(SQL_QUERY.format(filepath, tableN, DELIMITER[0], DELIMITER[1], DELIMITER[2]))
            logging.info(f"Pushed data for {tableN} from {filepath}.")
            
        mydb.commit()

    except Exception:
        logging.error(traceback.format_exc())
    
    logging.info("Stopped")
