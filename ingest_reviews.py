import json
import logging
import psycopg2
from datetime import datetime

# --------------------------------------------------
# LOGGING CONFIGURATION
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# --------------------------------------------------
# FILE LOCATION
# --------------------------------------------------
INPUT_FILE = r"C:\Bits-Sems\Bits-Sem2\DMLL\Assignment\dmml\data\raw\reviews\electronics_reviews.json"

# --------------------------------------------------
# DB CONFIG
# --------------------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "sajal"
}

# --------------------------------------------------
# INSERT SQL
# --------------------------------------------------
INSERT_SQL = """
INSERT INTO product_reviews (
    user_id, product_id, reviewer_name,
    helpful_yes, helpful_total,
    rating, review_summary, review_text,
    unix_review_time, review_date
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""

# --------------------------------------------------
def parse_review_date(review_time):
    """Convert Amazon reviewTime to DATE"""
    try:
        return datetime.strptime(review_time, "%m %d, %Y").date()
    except Exception:
        return None

# --------------------------------------------------
def main():
    logging.info("Starting Amazon reviews ingestion")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logging.info("Connected to PostgreSQL")
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return

    inserted_count = 0
    skipped_count = 0

    logging.info(f"Reading input file: {INPUT_FILE}")

    with open(INPUT_FILE, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            try:
                review = json.loads(line)

                values = (
                    review.get("reviewerID"),
                    review.get("asin"),
                    review.get("reviewerName"),
                    review.get("helpful", [0, 0])[0],
                    review.get("helpful", [0, 0])[1],
                    review.get("overall"),
                    review.get("summary"),
                    review.get("reviewText"),
                    review.get("unixReviewTime"),
                    parse_review_date(review.get("reviewTime"))
                )

                cursor.execute(INSERT_SQL, values)
                inserted_count += 1

                # Commit every 1000 rows (safe + faster)
                if inserted_count % 1000 == 0:
                    conn.commit()
                    logging.info(f"Inserted {inserted_count} reviews so far...")

            except Exception as e:
                skipped_count += 1
                logging.warning(
                    f"Skipped record at line {line_number}: {e}"
                )

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Ingestion completed successfully")
    logging.info(f"Total inserted: {inserted_count}")
    logging.info(f"Total skipped: {skipped_count}")

# --------------------------------------------------
if __name__ == "__main__":
    main()
