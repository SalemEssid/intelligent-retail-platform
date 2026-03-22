import random
import psycopg2
from datetime import datetime
import time
import logging

# -----------------------------
# 1️⃣ CONFIGURATION & LOGS
# -----------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------
# 2️⃣ CONNEXION DB & CHARGEMENT DES CLÉS
# -----------------------------
def get_connection():
    return psycopg2.connect(
        host="postgres"
,  # Nom du service dans docker-compose
        database="retail_data_platform",
        user="postgres",
        password="salemessid15122003"
    )

logger.info("Démarrage du Smart Data Generator...")
conn = get_connection()
cursor = conn.cursor()

# 🔥 LA CORRECTION EST ICI : Récupération des vrais IDs
logger.info("Chargement des dimensions en mémoire...")

cursor.execute("SELECT product_id FROM dim_product")
valid_products = [r[0] for r in cursor.fetchall()]

cursor.execute("SELECT customer_id FROM dim_customer")
valid_customers = [r[0] for r in cursor.fetchall()]

cursor.execute("SELECT store_id FROM dim_store")
valid_stores = [r[0] for r in cursor.fetchall()]

# Création des segments "Smart" basés sur les vrais IDs
popular_products = valid_products[:10] if len(valid_products) >= 10 else valid_products
loyal_customers = valid_customers[:10] if len(valid_customers) >= 10 else valid_customers

# -----------------------------
# 3️⃣ FONCTIONS INTELLIGENTES
# -----------------------------
def get_valid_date_id(cursor):
    try:
        cursor.execute("SELECT date_id FROM dim_date ORDER BY RANDOM() LIMIT 1")
        result = cursor.fetchone()
        return result[0] if result else 1
    except:
        return 1

def generate_smart_sale(sale_id, cursor):
    hour = datetime.now().hour
    
    # 🟢 Logique temporelle
    quantity = random.randint(1, 5) if 9 <= hour <= 21 else random.randint(1, 2)

    # 🟢 Biais Produits populaires
    if random.random() < 0.4:
        product_id = random.choice(popular_products)
    else:
        product_id = random.choice(valid_products) # <-- Utilisation de votre liste

    # 🟢 Biais Clients fidèles
    if random.random() < 0.3:
        customer_id = random.choice(loyal_customers)
    else:
        customer_id = random.choice(valid_customers) # <-- Utilisation de votre liste

    store_id = random.choice(valid_stores) # Sécurité pour les magasins aussi
    date_id = get_valid_date_id(cursor)
    
    discount = round(random.uniform(0, 0.3), 2)
    base_price = random.uniform(10, 200)
    revenue = round(base_price * quantity * (1 - discount), 2)

    return (sale_id, product_id, customer_id, store_id, date_id, quantity, revenue, discount)

def generate_smart_transaction(transaction_id, customer_id):
    amount = round(random.uniform(10, 500), 2)
    is_fraud = 0

    if random.random() < 0.05:
        amount = round(random.uniform(1000, 5000), 2)
        is_fraud = 1

    payment_method = random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"])
    
    return (transaction_id, customer_id, amount, payment_method, datetime.now(), is_fraud)

# -----------------------------
# 4️⃣ BOUCLE PRINCIPALE
# -----------------------------
if __name__ == "__main__":

    cursor.execute("SELECT COALESCE(MAX(sale_id), 10000) FROM fact_sales")
    sale_id = cursor.fetchone()[0] + 1

    cursor.execute("SELECT COALESCE(MAX(transaction_id), 1000) FROM fact_transactions")
    transaction_id = cursor.fetchone()[0] + 1

    try:
        for batch in range(5):  # ✅ finite number of batches
            for _ in range(10):  # 10 rows per batch

                sale = generate_smart_sale(sale_id, cursor)
                cursor.execute("""
                    INSERT INTO fact_sales (sale_id, product_id, customer_id, store_id, date_id, quantity, revenue, discount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, sale)

                customer_from_sale = sale[2]
                transaction = generate_smart_transaction(transaction_id, customer_from_sale)
                cursor.execute("""
                    INSERT INTO fact_transactions (transaction_id, customer_id, amount, payment_method, transaction_time, is_fraud)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, transaction)

                sale_id += 1
                transaction_id += 1

            conn.commit()
            logger.info(f"✅ Batch {batch+1}/5 inserted successfully")

    except psycopg2.Error as e:
        logger.error(f"Erreur DB: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
        logger.info("✅ Data generation finished.")
