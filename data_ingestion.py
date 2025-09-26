import pandas as pd
import os
from sqlalchemy import create_engine
import time
import logging

logging.basicConfig(
    filename= 'logs/ingestion_db.log',
    level = logging.DEBUG,
    format = '%(asctime)s - %(levelname)s - %(message)s',
    filemode ='a'
)

username = "root"          
password = "root" 
host = "localhost"         
port = "3306"              
database = "inventory"     

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")

def ingest_data(df, tbl_nm, engine, chunksize=5000, if_exists="append"):
    """
    Insert DataFrame into MySQL safely and quickly.

    Parameters:
        df (pd.DataFrame): Data to insert
        tbl_nm (str): Target table name
        engine: SQLAlchemy engine
        chunksize (int): Number of rows per batch insert (default 5000)
        if_exists (str): 'append' (default) | 'replace' | 'fail'
    """
    try:
        df.to_sql(
            name=tbl_nm,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,   # prevents memory errors
            method="multi"         # multi-row INSERTs (faster)
        )
        logging.info(f"Inserted {len(df)} rows into '{tbl_nm}'")
    except Exception as e:
        logging.info(f"Error inserting into {tbl_nm}: {e}")

def load_raw_data():
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            df = pd.read_csv('data/'+file)
            logging.info(f'Ingesting {file} in db.')
            ingest_data(df,file[:-4],engine)
    end = time.time()
    total_time = (end - start)/ 60
    logging.info('Ingestion complete.')
    logging.info(f'Total time taken for ingestion {total_time} in minutes.')

if __name__ == '__main__':
    load_raw_data()
