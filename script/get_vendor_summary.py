# import sqlite3
# import pandas as pd
# import logging
# #form ingestion_db import ingestion_db
# from ingestion_db import ingest_db

# logging.basicConfig(
# filename="logs/get_vendor_summary.log",
# level=logging.DEBUG,
# format="%(asctime)s - %(levelname)s - %(message)s",
# filemode="a"
# )

# def create_vendor_summary(conn):
#     """ this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data """
#     vendor_sales_summary = pd.read_sql_query("""
#     WITH FreightSummary AS (
#     SELECT
#         VendorNumber,
#         SUM(Freight) AS FreightCost
#     FROM vendor_invoice
#     GROUP BY VendorNumber
#     ),

#     PurchaseSummary AS (
#     SELECT
#         p.VendorNumber,
#         p.VendorName,
#         p.Brand,
#         p.Description,
#         p.PurchasePrice,
#         pp.Price AS ActualPrice,
#         pp.Volume,
#         SUM(p.Quantity) AS TotalPurchaseQuantity,
#         SUM(p.Dollars) AS TotalPurchaseDollars
#     FROM purchases p
#     JOIN purchase_prices pp
#         ON p.Brand = pp.Brand
#     WHERE p.PurchasePrice > 0
#     GROUP BY
#         p.VendorNumber,
#         p.VendorName,
#         p.Brand,
#         p.Description,
#         p.PurchasePrice,
#         pp.Price,
#         pp.Volume
#     ),

#     SalesSummary AS (
#     SELECT
#         VendorNo,
#         Brand,
#         SUM(SalesQuantity) AS TotalSalesQuantity,
#         SUM(SalesDollars) AS TotalSalesDollars,
#         SUM(SalesPrice) AS TotalSalesPrice,
#         SUM(ExciseTax) AS TotalExciseTax
#     FROM sales
#     GROUP BY VendorNo, Brand
#     )

#     SELECT
#     ps.VendorNumber,
#     ps.VendorName,
#     ps.Brand,
#     ps.Description,
#     ps.PurchasePrice,
#     ps.ActualPrice,
#     ps.Volume,
#     ps.TotalPurchaseQuantity,
#     ps.TotalPurchaseDollars,
#     ss.TotalSalesQuantity,
#     ss.TotalSalesDollars,
#     ss.TotalSalesPrice,
#     ss.TotalExciseTax,
#     fs.FreightCost
#     FROM PurchaseSummary ps
#     LEFT JOIN SalesSummary ss
#     ON ps.VendorNumber = ss.VendorNo
#     AND ps.Brand = ss.Brand
#     LEFT JOIN FreightSummary fs
#     ON ps.VendorNumber = fs.VendorNumber
#     ORDER BY ps.TotalPurchaseDollars DESC""", conn)
    
#     return vendor_sales_summary


# def clean_data(df):
#     """this function will clean the data"""
# # changing datatype to float
#     df['Volume'] =df ['Volume'].astype('float')

#     # filling missing value with 0
#     df.fillna(0,inplace = True)

# # removing spaces from categorical columns
#     df['VendorName'] = df['VendorName'].str.strip()
#     df['Description'] = df['Description'].str.strip()

# # creating new columns for better analysis
#     vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    
#     vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit'] / vendor_sales_summary['TotalSalesDollars'])*100
#     vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity'] / vendor_sales_summary['TotalPurchaseQuantity']
#     vendor_sales_summary['SalesToPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars']

#     return df 





# if __name__ =='__main__':
#     # creating database connection
#     conn = sqlite3.connect('inventory.db')

#     logging. info('Creating Vendor Summary Table ...')
#     summary_df = create_vendor_summary(conn)
#     logging.info(summary_df.head())

#     logging.info('Cleaning Data ..... ')
#     clean_df = clean_data(summary_df)
#     logging.info(clean_df.head())

#     logging.info('Ingesting data ..... ')
#     ingest_db(clean_df,'vendor_sales_summary',conn)
#     logging.info('Completed')
    
    
    
import sqlite3
import pandas as pd
import logging
import os
from ingestion_db import ingest_db

# Ensure logs and exports directories exist
os.makedirs("logs", exist_ok=True)
os.makedirs("exports", exist_ok=True)

# Set up logging
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    """Merge various tables to create vendor sales summary with additional metrics."""
    query = """
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
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
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price,
            pp.Volume
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
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """
    return pd.read_sql_query(query, conn)


def clean_data(df):
    """Clean and enhance the vendor summary DataFrame."""
    try:
        df['Volume'] = df['Volume'].astype(float)

        # Fill missing values with 0
        df.fillna(0, inplace=True)

        # Strip whitespace from string fields
        df['VendorName'] = df['VendorName'].str.strip()
        df['Description'] = df['Description'].str.strip()

        # Derived metrics
        df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
        df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars'].replace(0, 1)) * 100
        df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'].replace(0, 1)
        df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars'].replace(0, 1)

    except Exception as e:
        logging.error(f"Error during data cleaning: {e}")
        raise

    return df


if __name__ == '__main__':
    try:
        # Connect to SQLite DB
        conn = sqlite3.connect('inventory.db')

        logging.info('Creating Vendor Summary Table...')
        summary_df = create_vendor_summary(conn)
        logging.info(f'Vendor Summary Preview:\n{summary_df.head()}')

        logging.info('Cleaning Data...')
        clean_df = clean_data(summary_df)
        logging.info(f'Cleaned Data Preview:\n{clean_df.head()}')

        # Save to CSV inside exports folder
        csv_path = os.path.join("exports", "vendor_sales_summary.csv")
        clean_df.to_csv(csv_path, index=False)
        logging.info(f'Vendor sales summary saved as CSV at {csv_path}.')

        logging.info('Ingesting data into DB...')
        ingest_db(clean_df, 'vendor_sales_summary', conn)
        logging.info('Ingestion Complete.')

        conn.close()

    except Exception as e:
        logging.error(f"Script failed with error: {e}")
        raise

