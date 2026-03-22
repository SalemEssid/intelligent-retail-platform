import pandas as pd
import psycopg2
from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.cluster import KMeans
import pickle
import os
from pathlib import Path
from sklearn.metrics import mean_squared_error, mean_absolute_error
import numpy as np
from sklearn.metrics import silhouette_score
from sklearn.metrics import classification_report

# -----------------------------
# 1️⃣ DATABASE CONNECTION
# -----------------------------
conn = psycopg2.connect(
    host="localhost",
    database="retail_data_platform",
    user="postgres",
    password="salemessid15122003"
)

cursor = conn.cursor()
print("Connected to PostgreSQL")

# Setup paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
MODELS_DIR = SCRIPT_DIR.parent / "models"
MODELS_DIR.mkdir(exist_ok=True)

# -----------------------------
# 2️⃣ LOAD DATA FROM DATABASE
# -----------------------------
sales_query = """
SELECT sale_id, product_id, customer_id, quantity, revenue, discount
FROM fact_sales;
"""

transactions_query = """
SELECT transaction_id, customer_id, amount, is_fraud
FROM fact_transactions;
"""

sales_df = pd.read_sql(sales_query, conn)
transactions_df = pd.read_sql(transactions_query, conn)

print("Data loaded")

# -----------------------------
# 3️⃣ SALES PREDICTION MODEL
# -----------------------------
print("Training Sales Prediction Model...")

X_sales = sales_df[["quantity", "discount"]]
y_sales = sales_df["revenue"]

sales_model = RandomForestRegressor(n_estimators=100, random_state=42)
sales_model.fit(X_sales, y_sales)

sales_df["predicted_revenue"] = sales_model.predict(X_sales)

pickle.dump(sales_model, open(MODELS_DIR / "sales_model.pkl", "wb"))

print("Sales model trained")

rmse = np.sqrt(mean_squared_error(y_sales, sales_df["predicted_revenue"]))
mae = mean_absolute_error(y_sales, sales_df["predicted_revenue"])

print(f"Sales Model RMSE: {rmse}")
print(f"Sales Model MAE: {mae}")


# -----------------------------
# 4️⃣ CUSTOMER SEGMENTATION
# -----------------------------
print("Running Customer Segmentation...")

customer_data = sales_df.groupby("customer_id").agg({
    "revenue": "sum",
    "quantity": "sum"
}).reset_index()

kmeans = KMeans(n_clusters=3, random_state=42)
customer_data["segment"] = kmeans.fit_predict(
    customer_data[["revenue", "quantity"]]
)

pickle.dump(kmeans, open(MODELS_DIR / "kmeans_model.pkl", "wb"))

print("Segmentation done")
score = silhouette_score(
    customer_data[["revenue", "quantity"]],
    customer_data["segment"]
)

print(f"Segmentation Silhouette Score: {score}")


# -----------------------------
# 5️⃣ FRAUD DETECTION MODEL
# -----------------------------
print("Training Fraud Detection Model...")

X_fraud = transactions_df[["amount"]]

fraud_model = IsolationForest(contamination=0.05, random_state=42)
transactions_df["fraud_prediction"] = fraud_model.fit_predict(X_fraud)

# Convert (-1 = fraud, 1 = normal)
transactions_df["fraud_prediction"] = transactions_df["fraud_prediction"].map({
    1: 0,
    -1: 1
})

pickle.dump(fraud_model, open(MODELS_DIR / "fraud_model.pkl", "wb"))

print("Fraud model trained")
print("Fraud Detection Report:")
print(classification_report(
    transactions_df["is_fraud"],
    transactions_df["fraud_prediction"]
))


# -----------------------------
# 6️⃣ RECOMMENDATION SYSTEM
# -----------------------------
print("Building Recommendation System...")

# Create user-product matrix
basket = sales_df.pivot_table(
    index="customer_id",
    columns="product_id",
    values="quantity",
    fill_value=0
)

# Compute similarity (correlation)
correlation = basket.corr()

recommendations = []

for product in correlation.columns:
    similar_products = correlation[product].sort_values(ascending=False)[1:6]

    for rec_product, score in similar_products.items():
        recommendations.append((product, rec_product, float(score)))

print("Recommendations generated")

# -----------------------------
# 7️⃣ CREATE TABLES
# -----------------------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS predictions_sales (
    sale_id INT,
    predicted_revenue FLOAT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS customer_segments (
    customer_id VARCHAR(50),
    total_revenue FLOAT,
    total_quantity INT,
    segment INT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS fraud_predictions (
    transaction_id INT,
    fraud_prediction INT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS product_recommendations (
    product_id VARCHAR(50),
    recommended_product VARCHAR(50),
    score FLOAT
);
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS model_metrics (
    model_name VARCHAR(50),
    metric_name VARCHAR(50),
    metric_value FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
metrics = [
    ("sales_model", "rmse", float(rmse)),
    ("sales_model", "mae", float(mae)),
    ("segmentation", "silhouette_score", float(score))
]

for m in metrics:
    cursor.execute(
        "INSERT INTO model_metrics (model_name, metric_name, metric_value) VALUES (%s, %s, %s)",
        m
    )

conn.commit()
print("Tables ready")

# -----------------------------
# 8️⃣ CLEAN OLD DATA (IMPORTANT)
# -----------------------------
try:
    cursor.execute("DELETE FROM predictions_sales;")
    cursor.execute("DELETE FROM customer_segments;")
    cursor.execute("DELETE FROM fraud_predictions;")
    cursor.execute("DELETE FROM product_recommendations;")
    conn.commit()
except Exception as e:
    print(f"Error clearing old data: {e}")
    conn.rollback()

# -----------------------------
# 9️⃣ INSERT RESULTS
# -----------------------------
print("Inserting predictions...")

try:
    # Sales predictions
    for _, row in sales_df.iterrows():
        cursor.execute(
            "INSERT INTO predictions_sales VALUES (%s, %s)",
            (int(row["sale_id"]), float(row["predicted_revenue"]))
        )

    # Customer segments
    for _, row in customer_data.iterrows():
        cursor.execute(
            "INSERT INTO customer_segments VALUES (%s, %s, %s, %s)",
            (
                row["customer_id"],
                float(row["revenue"]),
                int(row["quantity"]),
                int(row["segment"])
            )
        )

    # Fraud predictions
    for _, row in transactions_df.iterrows():
        cursor.execute(
            "INSERT INTO fraud_predictions VALUES (%s, %s)",
            (
                int(row["transaction_id"]),
                int(row["fraud_prediction"])
            )
        )

    # Product recommendations
    for rec in recommendations:
        cursor.execute(
            "INSERT INTO product_recommendations VALUES (%s, %s, %s)",
            rec
        )

    conn.commit()
    print("All predictions inserted!")
except Exception as e:
    print(f"Error inserting predictions: {e}")
    conn.rollback()

# -----------------------------
# 🔟 CLOSE CONNECTION
# -----------------------------
cursor.close()
conn.close()

print("Full ML Pipeline completed successfully 🚀")
