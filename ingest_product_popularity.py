import json
import logging
import psycopg2
import time

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

INPUT_FILE = r"C:\Bits-Sems\Bits-Sem2\DMLL\Assignment\dmml\data\raw\external_api\product_popularity.json"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "sajal"
}

INSERT_METADATA_SQL = """
INSERT INTO product_popularity_metadata (
    run_id,
    total_products,
    generated_at,
    source,
    popularity_algorithm,
    created_at
)
VALUES (%s, %s, %s, %s, %s, NOW());
"""

INSERT_PRODUCT_SQL = """
INSERT INTO product_popularity (
    run_id,
    product_id,
    popularity_score,
    avg_rating,
    review_count,
    last_updated,
    created_at
)
VALUES (%s, %s, %s, %s, %s, %s, NOW());
"""

def main():
    logging.info("Starting product popularity data ingestion")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logging.info("Connected to PostgreSQL")
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return

    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load JSON file: {e}")
        return

    metadata = data.get("metadata", {})
    products = data.get("products", [])

    if not metadata or not products:
        logging.error("Missing metadata or products in JSON")
        return

    # ✅ Generate run_id manually (Unix timestamp)
    run_id = int(time.time())

    inserted_products = 0
    failed_products = 0

    try:
        # Insert metadata WITH run_id
        cursor.execute(INSERT_METADATA_SQL, (
            run_id,
            metadata.get("total_products"),
            metadata.get("generated_at"),
            metadata.get("source"),
            metadata.get("popularity_algorithm")
        ))
        logging.info(f"Metadata inserted with run_id: {run_id}")

        # Insert products
        for product in products:
            try:
                cursor.execute(INSERT_PRODUCT_SQL, (
                    run_id,
                    product["product_id"],
                    product["popularity_score"],
                    product["avg_rating"],
                    product["review_count"],
                    product["last_updated"]
                ))
                inserted_products += 1

                if inserted_products % 1000 == 0:
                    conn.commit()
                    logging.info(f"{inserted_products} products inserted")

            except Exception as prod_error:
                failed_products += 1
                logging.error(f"Failed product {product.get('product_id')}: {prod_error}")
                conn.rollback()

        conn.commit()
        logging.info(f"Ingestion complete. Total products inserted: {inserted_products}, failed: {failed_products}")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()