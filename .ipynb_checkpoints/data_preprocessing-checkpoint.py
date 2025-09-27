import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import time
from data_ingestion import ingest_data  # make sure data_ingestion.py is in same folder
import os
import logging

# Ensure logs folder exists
os.makedirs('logs', exist_ok=True)

# Dedicated logger for preprocessing
logger = logging.getLogger("preprocessing")
logger.setLevel(logging.DEBUG)
if logger.hasHandlers():
    logger.handlers.clear()
fh = logging.FileHandler("logs/preprocessing.log")
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

def get_vendor_summary(engine):
    logger.info("Starting vendor summary extraction from DB")
    start_time = time.time()
    try:
        vendor_sales_summary = pd.read_sql_query("""
        WITH FreightSummary AS(
            SELECT VendorNumber, SUM(Freight) AS FreightCost
            FROM vendor_invoice
            GROUP BY VendorNumber
        ),
        PurchaseSummary AS (
            SELECT
                p.VendorNumber,
                p.VendorName,
                p.Brand,
                p.Description,
                p.PurchasePrice,
                pp.Price AS ActualPrice,
                pp.Volume,
                SUM(p.Quantity) AS TotalPurchaseQuantity,
                SUM(p.Dollars) AS TotalPurchaseDollars
            FROM purchases p
            JOIN purchase_prices pp ON p.Brand = pp.Brand
            WHERE p.PurchasePrice > 0
            GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
        ),
        SalesSummary AS (
            SELECT
                VendorNo,
                Brand,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(SalesDollars) AS TotalSalesDollars,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand
        )
        SELECT
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description,
            ps.PurchasePrice,
            ps.ActualPrice,
            ps.Volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity,
            ss.TotalSalesDollars,
            ss.TotalSalesPrice,
            ss.TotalExciseTax,
            fs.FreightCost
        FROM PurchaseSummary ps
        LEFT JOIN SalesSummary ss ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
        LEFT JOIN FreightSummary fs ON ps.VendorNumber = fs.VendorNumber
        ORDER BY ps.TotalPurchaseDollars DESC
        """, engine)
        logger.info(f"Vendor summary extraction completed. Rows fetched: {len(vendor_sales_summary)}")
    except Exception as e:
        logger.error(f"Error fetching vendor summary: {e}")
        raise
    end_time = time.time()
    logger.info(f"Time taken for query execution: {end_time - start_time:.2f} seconds")
    return vendor_sales_summary

def clean_data(df):
    logger.info("Starting data cleaning")
    try:
        df.Volume = df.Volume.astype('float64')
        df.fillna(0, inplace=True)
        df.VendorName = df.VendorName.str.strip()
        df.Description = df.Description.str.strip()
        
        df['GrossProfit'] = df.TotalSalesDollars - df.TotalPurchaseDollars
        df['ProfitMargin'] = (df.GrossProfit / df.TotalSalesDollars) * 100
        df['StockTurnover'] = df.TotalSalesQuantity / df.TotalPurchaseQuantity
        df['SalestoPurchaseRatio'] = df.TotalSalesDollars / df.TotalPurchaseDollars
        
        inf_count = np.isinf(df.select_dtypes(include=[np.number])).sum().sum()
        if inf_count > 0:
            logger.warning(f"Found {inf_count} inf/-inf values. Replacing with 0")
        df.replace([np.inf, -np.inf], 0, inplace=True)
        df.fillna(0, inplace=True)
        
        logger.info("Data cleaning completed successfully")
    except Exception as e:
        logger.error(f"Error during data cleaning: {e}")
        raise
    return df

if __name__ == '__main__':
    logger.info("Preprocessing script started")
    try:
        summary_df = get_vendor_summary(engine)
        clean_df = clean_data(summary_df)
        logger.info("Starting ingestion into database")
        ingest_data(clean_df, 'vendor_sales_summary', engine)
        logger.info("Data ingestion completed successfully")
    except Exception as e:
        logger.error(f"Script terminated with error: {e}")
