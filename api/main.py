from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pickle
import logging
from pathlib import Path
import psycopg2
import pandas as pd
import os

# -----------------------------
# 1️⃣ CONFIG
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Retail ML API",
    description="API for predictions and analytics",
    version="1.0.0"
)

# Allow dashboard access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Paths
API_DIR = Path(__file__).parent
PROJECT_ROOT = API_DIR.parent
ML_DIR = PROJECT_ROOT / "ml" / "models"

# Models
sales_model = None
fraud_model = None
kmeans_model = None

# -----------------------------
# 2️⃣ LOAD MODELS
# -----------------------------
def load_models():
    global sales_model, fraud_model, kmeans_model

    try:
        sales_model_path = ML_DIR / "sales_model.pkl"
        fraud_model_path = ML_DIR / "fraud_model.pkl"
        kmeans_model_path = ML_DIR / "kmeans_model.pkl"

        if not sales_model_path.exists():
            raise FileNotFoundError("sales_model.pkl not found")
        if not fraud_model_path.exists():
            raise FileNotFoundError("fraud_model.pkl not found")
        if not kmeans_model_path.exists():
            raise FileNotFoundError("kmeans_model.pkl not found")

        sales_model = pickle.load(open(sales_model_path, "rb"))
        fraud_model = pickle.load(open(fraud_model_path, "rb"))
        kmeans_model = pickle.load(open(kmeans_model_path, "rb"))

        logger.info("✅ Models loaded successfully")

    except Exception as e:
        logger.error(f"❌ Error loading models: {e}")
        raise


@app.on_event("startup")
def startup():
    load_models()


# -----------------------------
# 3️⃣ DATABASE CONNECTION
# -----------------------------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database="retail_data_platform",
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "salemessid15122003")
    )


# -----------------------------
# 4️⃣ SALES PREDICTION
# -----------------------------
@app.get("/predict_sales")
def predict_sales(quantity: float, discount: float):
    try:
        if sales_model is None:
            raise HTTPException(status_code=500, detail="Model not loaded")

        pred = sales_model.predict([[quantity, discount]])

        return {
            "quantity": quantity,
            "discount": discount,
            "predicted_revenue": float(pred[0])
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 5️⃣ FRAUD DETECTION
# -----------------------------
@app.get("/detect_fraud")
def detect_fraud(amount: float):
    try:
        if fraud_model is None:
            raise HTTPException(status_code=500, detail="Model not loaded")

        pred = fraud_model.predict([[amount]])
        result = 1 if pred[0] == -1 else 0

        return {
            "amount": amount,
            "fraud": result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 6️⃣ CUSTOMER SEGMENTATION (USED BY DASHBOARD)
# -----------------------------
@app.get("/customer_segments")
def get_customer_segments():
    try:
        if kmeans_model is None:
            raise HTTPException(status_code=500, detail="Model not loaded")

        conn = get_connection()

        query = """
            SELECT customer_id, SUM(revenue) AS total_spent, COUNT(*) AS frequency
            FROM fact_sales
            GROUP BY customer_id
        """

        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            return []

        try:
            X = df[['total_spent', 'frequency']].values
            df['segment'] = kmeans_model.predict(X)
        except Exception as e:
            logger.error(f"KMEANS ERROR: {e}")
            df['segment'] = 0


        # Rename for dashboard
        df = df.rename(columns={
            "total_spent": "feature_1",
            "frequency": "feature_2"
        })

        return df[['customer_id', 'feature_1', 'feature_2', 'segment']].to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error in segmentation: {e}")
        return []


# -----------------------------
# 7️⃣ HEALTH CHECK
# -----------------------------
@app.get("/")
def home():
    return {
        "status": "ok",
        "message": "Retail ML API is running 🚀"
    }


@app.get("/health")
def health():
    db_ok = False
    try:
        conn = get_connection()
        conn.close()
        db_ok = True
    except Exception as e:
        logger.error(f"Health DB check failed: {e}")

    models_ok = all([sales_model is not None, fraud_model is not None, kmeans_model is not None])
    overall = "ok" if db_ok and models_ok else "degraded"

    return {
        "status": overall,
        "database": db_ok,
        "models": {
            "sales_model": sales_model is not None,
            "fraud_model": fraud_model is not None,
            "kmeans_model": kmeans_model is not None
        }
    }
