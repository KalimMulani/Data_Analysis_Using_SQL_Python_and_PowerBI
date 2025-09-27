import pandas as pd
import os
from sqlalchemy import create_engine
import time
import logging

# Ensure logs folder exists
os.makedirs('logs', exist_ok=True)

# Dedicated logger for ingestion
logger = logging.getLogger("ingestion")
logger.setLevel(logging.DEBUG)
if logger.hasHandlers():
    logger.handlers.clear()
fh = logging.FileHandler("logs/ingestion_db.log")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# MySQL connection
username = "root"
password = "root"
host = "localhost"
port = "3306"
database = "inventory"

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")

def ingest_data(df, tbl_nm, engine, chunksize=5000, if_exists="replace"):
    """
    Insert DataFrame into MySQL safely and quickly.
    """
    try:
        df.to_sql(
            name=tbl_nm,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
            method="multi"
        )
        logger.info(f"Inserted {len(df)} rows into '{tbl_nm}'")
    except Exception as e:
        logger.error(f"Error inserting into {tbl_nm}: {e}")
        raise

def load_raw_data():
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join('data', file))
            logger.info(f'Ingesting {file} into database.')
            ingest_data(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start)/60
    logger.info('Ingestion complete.')
    logger.info(f'Total time taken for ingestion: {total_time:.2f} minutes.')

if __name__ == '__main__':
    load_raw_data()
