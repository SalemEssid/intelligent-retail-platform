import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import os

def train_sales_model():  # <--- This is the "symbol" VS Code is looking for
    # Use paths relative to the project root for Airflow
    data_path = "data/sales_data.csv"
    
    if not os.path.exists(data_path):
        print(f"File not found: {data_path}")
        return

    # Load data
    df = pd.read_csv(data_path)

    # Features
    X = df[["quantity", "discount"]]
    y = df["price"] * df["quantity"]

    # Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    # Model
    model = RandomForestRegressor(n_estimators=100)
    model.fit(X_train, y_train)

    print("Sales Model trained successfully!")
    # You might want to save the model here
    # import joblib
    # joblib.dump(model, "models/sales_model.pkl")

if __name__ == "__main__":
    # This allows you to still run the script manually for testing
    train_sales_model()