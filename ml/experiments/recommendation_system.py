import pandas as pd
import os

def train_recommender(): # <--- This name must match your DAG import!
    # Path relative to project root
    file_path = "data/sales_data.csv"
    
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    df = pd.read_csv(file_path)

    # Creating the basket matrix
    basket = df.pivot_table(
        index="customer_id",
        columns="product_id",
        values="quantity",
        fill_value=0
    )

    # Calculating correlation between products
    correlation = basket.corr()

    # For testing: check recommendations for a specific product
    test_product = correlation.columns[0] # Get the first available ID
    recommendations = correlation[test_product].sort_values(ascending=False)

    print(f"Recommendation Model trained. Top matches for {test_product}:")
    print(recommendations.head(5))
    
    # In a real project, you'd save the 'correlation' matrix here
    # correlation.to_pickle("models/recommender_matrix.pkl")

if __name__ == "__main__":
    train_recommender()
