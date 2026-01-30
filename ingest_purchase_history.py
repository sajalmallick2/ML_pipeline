import csv
import logging
from datetime import datetime
import psycopg2

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# -----------------------------
# FILE PATH
# -----------------------------
INPUT_FILE = r"C:\Bits-Sems\Bits-Sem2\DMLL\Assignment\dmml\data\raw\transactions\purchase_history.csv"

# -----------------------------
# DB CONFIG
# -----------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "sajal"
}

# -----------------------------
# INSERT SQL
# -----------------------------
INSERT_SQL = """
INSERT INTO purchase_history (
    transaction_id,
    user_id,
    product_id,
    transaction_date,
    transaction_time,
    quantity,
    price,
    rating
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING;
"""

# -----------------------------
def main():
    logging.info("Starting transaction ingestion")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logging.info("Connected to PostgreSQL")
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return

    inserted = 0
    failed = 0

    with open(INPUT_FILE, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            try:
                # Parse date
                transaction_date = datetime.strptime(
                    row["transaction_date"], "%m %d, %Y"
                ).date()

                # Parse time from Unix timestamp
                unix_time = int(row["transaction_time"])
                dt = datetime.utcfromtimestamp(unix_time)
                transaction_time = dt.time()

                # Prepare values
                values = (
                    row["transaction_id"],           # TEXT (or VARCHAR)
                    row["user_id"],
                    row["product_id"],
                    transaction_date,
                    transaction_time,                # time without time zone
                    int(row["quantity"]),
                    float(row["price"]),
                    float(row["rating"])
                )

                cursor.execute(INSERT_SQL, values)
                inserted += 1

                if inserted % 1000 == 0:
                    conn.commit()
                    logging.info(f"{inserted} records inserted")

            except Exception as row_error:
                failed += 1
                logging.error(f"Failed row {row['transaction_id']}: {row_error}")
                #Rollback the failed transaction to continue
                conn.rollback()

    # Final commit
    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"Ingestion complete. Total records inserted: {inserted}, failed: {failed}")

# -----------------------------
if __name__ == "__main__":
    main()