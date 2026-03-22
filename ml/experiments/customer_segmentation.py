import pandas as pd
from sklearn.cluster import KMeans
import os

def train_kmeans(): # <--- This name must match your DAG import!
    # It's safer to use absolute-ish paths in Airflow
    file_path = "data/sales_data.csv"
    
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    df = pd.read_csv(file_path)

    customer_data = df.groupby("customer_id").agg({
        "price": "sum",
        "quantity": "sum"
    }).reset_index()

    X = customer_data[["price", "quantity"]]

    kmeans = KMeans(n_clusters=3, n_init='auto') # Added n_init to avoid warnings
    customer_data["segment"] = kmeans.fit_predict(X)

    print("K-Means training completed.")
    print(customer_data.head())
    
    # Optional: Save the results
    # customer_data.to_csv("data/processed/customer_segments.csv", index=False)

if __name__ == "__main__":
    train_kmeans()