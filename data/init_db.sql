-- 1. Dimensions
CREATE TABLE IF NOT EXISTS dim_product (
    product_id SERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id SERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id SERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    order_date DATE
);

-- 2. Fact Tables (Updated to match Python logic)
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id INT PRIMARY KEY,
    product_id INT,
    customer_id INT,
    store_id INT,
    date_id INT,
    quantity INT,
    revenue FLOAT,
    discount FLOAT
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    amount FLOAT,
    payment_method VARCHAR(50),
    transaction_time TIMESTAMP,
    is_fraud INT
);

-- 3. Initial Data
INSERT INTO dim_product (product_id) VALUES (1), (2), (3), (4), (5)
ON CONFLICT (product_id) DO NOTHING;
INSERT INTO dim_customer (customer_id) VALUES (1), (2), (3), (4), (5)
ON CONFLICT (customer_id) DO NOTHING;
INSERT INTO dim_store (store_id) VALUES (1), (2)
ON CONFLICT (store_id) DO NOTHING;
INSERT INTO dim_date (date_id, order_date) VALUES (1, '2024-01-01'), (2, '2024-01-02')
ON CONFLICT (date_id) DO NOTHING;