import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
import requests
import os

# -----------------------------
# 1️⃣ Connect to PostgreSQL
# -----------------------------
from psycopg2 import pool

db_pool = pool.SimpleConnectionPool(
    1, 5,
    host=os.getenv("DB_HOST", "localhost"),
    database="retail_data_platform",
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "salemessid15122003")
)

def get_conn():
    return db_pool.getconn()

def release_conn(conn):
    db_pool.putconn(conn)

# -----------------------------
# 2️⃣ Dash App Initialization
# -----------------------------
app = dash.Dash(__name__)
app.title = "Intelligent Retail Dashboard"

# -----------------------------
# 3️⃣ Layout
# -----------------------------
app.layout = html.Div([
    html.H1("Intelligent Retail Data Platform", style={'textAlign': 'center'}),

    dcc.Interval(id='interval-component', interval=60*1000, n_intervals=0),  # refresh every 60 seconds

    html.Div([
        html.H2("Sales Revenue Over Time"),
        dcc.Graph(id='sales-over-time')
    ]),

    html.Div([
        html.H2("Fraudulent Transactions"),
        dcc.Graph(id='fraud-over-time')
    ]),

    html.Div([
        html.H2("Top Products"),
        dcc.Graph(id='top-products')
    ]),

    html.Div([
        html.H2("Customer Segmentation Sample"),
        dcc.Graph(id='customer-segments')
    ])
])

# -----------------------------
# 4️⃣ Callbacks for Real-Time Updates
# -----------------------------
@app.callback(
    Output('sales-over-time', 'figure'),
    Output('fraud-over-time', 'figure'),
    Output('top-products', 'figure'),
    Output('customer-segments', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_dashboard(n):
    conn = get_conn()
    try:
        # --- Sales over time ---
        sales_query = """
            SELECT d.order_date, SUM(f.revenue) AS total_revenue
            FROM fact_sales f
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.order_date
            ORDER BY d.order_date
        """
        sales_df = pd.read_sql(sales_query, conn)
        if sales_df.empty:
            fig_sales = px.line(title="No sales data")
        else:
            fig_sales = px.line(sales_df, x='order_date', y='total_revenue', title='Revenue Over Time')

        # --- Fraud overview ---
        fraud_query = """
            SELECT DATE(transaction_time) AS txn_date, COUNT(*) FILTER (WHERE is_fraud = 1) AS fraud_count
            FROM fact_transactions
            GROUP BY DATE(transaction_time)
            ORDER BY txn_date
        """
        fraud_df = pd.read_sql(fraud_query, conn)
        if fraud_df.empty:
            fig_fraud = px.bar(title="No fraud data")
        else:
            fig_fraud = px.bar(fraud_df, x='txn_date', y='fraud_count', title='Fraudulent Transactions Over Time')

        # --- Top Products ---
        top_products_query = """
            SELECT f.product_id, SUM(f.revenue) AS total_revenue
            FROM fact_sales f
            GROUP BY f.product_id
            ORDER BY total_revenue DESC
            LIMIT 10
        """
        top_products_df = pd.read_sql(top_products_query, conn)
        if top_products_df.empty:
            fig_products = px.bar(title="No product data")
        else:
            fig_products = px.bar(top_products_df, x='product_id', y='total_revenue', title='Top 10 Products by Revenue')

        # --- Customer Segments (optional API call) ---
        try:
            response = requests.get("http://127.0.0.1:8000/customer_segments", timeout=3)
            response.raise_for_status()
            customer_data = response.json()
            cust_df = pd.DataFrame(customer_data)
            fig_segments = px.scatter(cust_df, x='feature_1', y='feature_2', color='segment', title='Customer Segments')
        except Exception:
            fig_segments = px.scatter(title='Customer Segments (API not available)')

        return fig_sales, fig_fraud, fig_products, fig_segments
    finally:
        release_conn(conn)

# -----------------------------
# 5️⃣ Run Server
# -----------------------------
if __name__ == '__main__':
    app.run(debug=True, port=8050)
