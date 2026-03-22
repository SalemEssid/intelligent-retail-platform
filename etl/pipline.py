import psycopg2
from psycopg2.extras import execute_values


# Connect to default database to check/create retail_db
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="salemessid15122003"
)
conn.autocommit = True
cursor = conn.cursor()
cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'retail_data_platform'")
if not cursor.fetchone():
    cursor.execute("CREATE DATABASE retail_data_platform")
conn.close()

# Now connect to retail_data_platform
conn = psycopg2.connect(
    host="localhost",
    database="retail_data_platform",
    user="postgres",
    password="salemessid15122003"
)

cursor = conn.cursor()
import pandas as pd

sales_df = pd.read_csv("data/sales_data.csv")
fraud_df = pd.read_csv("data/transactions_fraud.csv")
dim_customer = sales_df[[
    "customer_id", "customer_name", "age", "gender", "city"
]].drop_duplicates(subset="customer_id")

dim_customer.columns = ["customer_id", "name", "age", "gender", "city"]
dim_product = sales_df[[
    "product_id", "product_name", "category", "brand", "price"
]].drop_duplicates(subset="product_id")

dim_store = sales_df[[
    "store_id", "store_name", "region", "city"
]].drop_duplicates(subset="store_id")

sales_df["order_date"] = pd.to_datetime(sales_df["order_date"])

dim_date = sales_df[["order_date"]].drop_duplicates()

dim_date["date_id"] = dim_date["order_date"].rank(method="dense").astype(int)
dim_date["day"] = dim_date["order_date"].dt.day
dim_date["month"] = dim_date["order_date"].dt.month
dim_date["year"] = dim_date["order_date"].dt.year
dim_date["weekday"] = dim_date["order_date"].dt.day_name()
dim_date = dim_date[[
    "date_id", "order_date", "day", "month", "year", "weekday"
]]


fact_sales = sales_df.merge(dim_date, on="order_date")

fact_sales["revenue"] = fact_sales["price"] * fact_sales["quantity"]

fact_sales = fact_sales[[
    "transaction_id",
    "product_id",
    "customer_id",
    "store_id",
    "date_id",
    "quantity",
    "revenue",
    "discount"
]]

fact_sales.columns = [
    "sale_id", "product_id", "customer_id",
    "store_id", "date_id", "quantity",
    "revenue", "discount"
]

fact_transactions = fraud_df.copy()

cursor.execute('DROP TABLE IF EXISTS fact_sales CASCADE')
cursor.execute('DROP TABLE IF EXISTS fact_transactions CASCADE')
cursor.execute('DROP TABLE IF EXISTS dim_customer CASCADE')
cursor.execute('''
CREATE TABLE dim_customer (
    customer_id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255),
    age INTEGER,
    gender VARCHAR(10),
    city VARCHAR(255)
)
''')

cursor.execute('DROP TABLE IF EXISTS dim_product CASCADE')
cursor.execute('''
CREATE TABLE dim_product (
    product_id VARCHAR(10) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(255),
    brand VARCHAR(255),
    price DECIMAL(10,2)
)
''')

cursor.execute('DROP TABLE IF EXISTS dim_store CASCADE')
cursor.execute('''
CREATE TABLE dim_store (
    store_id VARCHAR(10) PRIMARY KEY,
    store_name VARCHAR(255),
    region VARCHAR(255),
    city VARCHAR(255)
)
''')

cursor.execute('DROP TABLE IF EXISTS dim_date CASCADE')
cursor.execute('''
CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY,
    order_date DATE,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR(20)
)
''')

cursor.execute('''
CREATE TABLE fact_sales (
    sale_id INTEGER PRIMARY KEY,
    product_id VARCHAR(10),
    customer_id VARCHAR(10),
    store_id VARCHAR(10),
    date_id INTEGER,
    quantity INTEGER,
    revenue DECIMAL(10,2),
    discount DECIMAL(10,2),

    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);
''')


cursor.execute('''
CREATE TABLE fact_transactions (
    transaction_id INTEGER PRIMARY KEY,
    customer_id VARCHAR(10),
    amount DECIMAL(10,2),
    payment_method VARCHAR(50),
    transaction_time TIMESTAMP,
    is_fraud SMALLINT
)
''')
cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_product ON fact_sales(product_id);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_customer ON fact_sales(customer_id);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_customer ON fact_transactions(customer_id);")

fact_sales = fact_sales.dropna(subset=["sale_id"])
fact_transactions = fact_transactions.dropna(subset=["transaction_id"])

def insert_bulk(table, df):
    columns = list(df.columns)
    values = list(df.itertuples(index=False, name=None))


    query = f"""
    INSERT INTO {table} ({','.join(columns)})
    VALUES %s
    """

    execute_values(cursor, query, values)

insert_bulk("dim_customer", dim_customer)
print("Inserting dim_customer...")
insert_bulk("dim_product", dim_product)
print("Inserting dim_product...")
insert_bulk("dim_store", dim_store)
print("Inserting dim_store...")
insert_bulk("dim_date", dim_date)
print("Inserting dim_date...")
insert_bulk("fact_sales", fact_sales)
print("Inserting fact_sales...")
insert_bulk("fact_transactions", fact_transactions)
print("Inserting fact_transactions...")

conn.commit()
conn.close()