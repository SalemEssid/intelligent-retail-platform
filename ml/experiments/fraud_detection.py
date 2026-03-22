import pandas as pd
from sklearn.ensemble import IsolationForest
import os

def train_fraud_model(): # <--- This name must match your DAG import!
    # Path relative to project root for Airflow
    file_path = "data/transactions_fraud.csv"
    
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    df = pd.read_csv(file_path)

    # We use 'amount' as the feature for isolation forest
    X = df[["amount"]]

    # Contamination 0.05 means we expect 5% of data to be outliers (fraud)
    model = IsolationForest(contamination=0.05, random_state=42)
    df["anomaly"] = model.fit_predict(X)

    # -1 = fraud, 1 = normal
    fraud_count = len(df[df["anomaly"] == -1])
    print(f"Fraud model trained. Detected {fraud_count} potential anomalies.")
    print(df.head())

if __name__ == "__main__":
    train_fraud_model()