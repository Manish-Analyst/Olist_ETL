# olist_etl.py

import os
import time
import logging
import warnings
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# --- Setup ---
warnings.filterwarnings('ignore')

# Logging setup
LOG_FILE = 'olist.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Load environment variables
load_dotenv()

def get_db_config(prefix):
    return {
        'DB_USER': os.getenv("DB_USER"),
        'DB_PASS': os.getenv("DB_PASS"),
        'DB_HOST': os.getenv("DB_HOST"),
        'DB_PORT': os.getenv("DB_PORT"),
        'DB_NAME': os.getenv(f"{prefix}_NAME")
    }

# Database Configs
DB_SOURCE = get_db_config("DBS")
DB_TARGET = get_db_config("DBT")

# Create SQLAlchemy engine
def get_engine(cfg):
    return create_engine(
        f"postgresql+psycopg2://{cfg['DB_USER']}:{cfg['DB_PASS']}@{cfg['DB_HOST']}:{cfg['DB_PORT']}/{cfg['DB_NAME']}"
    )

source_engine = get_engine(DB_SOURCE)
target_engine = get_engine(DB_TARGET)

# --- Extract Phase ---

def extract_all_tables(engine):
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    tables = pd.read_sql(query, engine)['table_name'].tolist()
    return {table: pd.read_sql(f"SELECT * FROM {table}", engine) for table in tables}

dfs = extract_all_tables(source_engine)

# --- Transform Phase ---

def transform_dim_customers(df):
    df = df.drop_duplicates(subset='customer_id')
    df['customer_unique_id'] = df['customer_unique_id'].astype(str)
    return df

def transform_dim_sellers(df):
    return df.drop_duplicates(subset='seller_id')

def transform_dim_products(products_df, translation_df):
    merged = products_df.merge(translation_df, on='product_category_name', how='left')
    return merged.drop_duplicates(subset='product_id')

def transform_dim_date(orders_df):
    orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'])
    dim_date = pd.DataFrame({
        'date': pd.to_datetime(orders_df['order_purchase_timestamp'].dt.date).drop_duplicates()
    })
    dim_date['day'] = dim_date['date'].dt.day
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['weekday'] = dim_date['date'].dt.weekday
    return dim_date

def transform_fact_orders(dfs):
    orders = dfs['olist_orders_dataset']
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])

    merged = (
        orders
        .merge(dfs['olist_order_items_dataset'], on='order_id', how='left')
        .merge(dfs['olist_order_payments_dataset'], on='order_id', how='left')
        .merge(dfs['olist_order_reviews_dataset'], on='order_id', how='left')
    )

    cols = [
        'order_id', 'customer_id', 'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date', 'product_id', 'seller_id', 'price',
        'freight_value', 'payment_type', 'payment_installments',
        'payment_value', 'review_score'
    ]

    merged = merged[cols]
    merged['payment_value'] = pd.to_numeric(merged['payment_value'], errors='coerce')
    merged['review_score'] = pd.to_numeric(merged['review_score'], errors='coerce')
    return merged

# --- Load Phase ---

def load_table(df, table_name, engine=target_engine):
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    logging.info(f"Loaded table: {table_name}")
    print(f"Loaded: {table_name}")

# --- Run ETL ---

def main():
    try:
        # Transform
        dim_customers = transform_dim_customers(dfs['olist_customers_dataset'])
        dim_sellers = transform_dim_sellers(dfs['olist_sellers_dataset'])
        dim_products = transform_dim_products(
            dfs['olist_products_dataset'], dfs['product_category_name_translation']
        )
        dim_date = transform_dim_date(dfs['olist_orders_dataset'])
        fact_orders = transform_fact_orders(dfs)

        # Load
        load_table(dim_customers, 'dim_customers')
        load_table(dim_sellers, 'dim_sellers')
        load_table(dim_products, 'dim_products')
        load_table(dim_date, 'dim_date')
        load_table(fact_orders, 'fact_orders')

    except Exception as e:
        logging.exception("ETL job failed")
        print("ETL job failed. Check log for details.")

if __name__ == "__main__":
    main()
