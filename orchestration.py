from prefect import flow, task
from prefect.logging import get_run_logger
import subprocess
import os

# ----------------------------
# Define Tasks
# ----------------------------

@task(name="Ingest Reviews")
def ingest_reviews():
    logger = get_run_logger()
    logger.info("Ingesting reviews into PostgreSQL...")
    result = subprocess.run(
        ["python", "ingest_reviews.py"],
        capture_output=True,
        text=True
    )
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            if line:
                logger.info(f"[ingest_reviews] {line}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            if line:
                logger.error(f"[ingest_reviews] {line}")
    if result.returncode != 0:
        raise Exception("Reviews ingestion failed")
    logger.info("Reviews ingested")

@task(name="Ingest Purchase History")
def ingest_purchase_history():
    logger = get_run_logger()
    logger.info("Ingesting purchase history into PostgreSQL...")
    result = subprocess.run(
        ["python", "ingest_purchase_history.py"],
        capture_output=True,
        text=True
    )
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            if line:
                logger.info(f"[ingest_purchase_history] {line}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            if line:
                logger.error(f"[ingest_purchase_history] {line}")
    if result.returncode != 0:
        raise Exception("Purchase history ingestion failed")
    logger.info("Purchase history ingested")

@task(name="Ingest Product Popularity")
def ingest_product_popularity():
    logger = get_run_logger()
    logger.info("Ingesting product popularity into PostgreSQL...")
    result = subprocess.run(
        ["python", "ingest_product_popularity.py"],
        capture_output=True,
        text=True
    )
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            if line:
                logger.info(f"[ingest_product_popularity] {line}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            if line:
                logger.error(f"[ingest_product_popularity] {line}")
    if result.returncode != 0:
        raise Exception("Product popularity ingestion failed")
    logger.info("Product popularity ingested")

@task(name="Merge Data")
def merge_data():
    logger = get_run_logger()
    logger.info("Merging data from PostgreSQL...")
    result = subprocess.run(
        ["python", "src/merge_data.py"],
        capture_output=True,
        text=True
    )
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            if line:
                logger.info(f"[merge_data] {line}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            if line:
                logger.error(f"[merge_data] {line}")
    if result.returncode != 0:
        raise Exception("Data merge failed")
    logger.info("Merged data saved")

@task(name="Validate Data")
def validate_data():
    logger = get_run_logger()
    logger.info("Validating merged data...")
    result = subprocess.run(
        ["python", "src/data_validation.py"],
        capture_output=True,
        text=True
    )
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            if line:
                logger.info(f"[data_validation] {line}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            if line:
                logger.error(f"[data_validation] {line}")
    if result.returncode != 0:
        raise Exception("Data validation failed")
    logger.info("Data validation passed")

@task(name="Profile Data")
def profile_data():
    logger = get_run_logger()
    logger.info("Profiling merged and validated data...")
    result = subprocess.run(
        ["python", "src/data_profiling.py"],  # Make sure this script exists
        capture_output=True,
        text=True
    )
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            if line:
                logger.info(f"[data_profiling] {line}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            if line:
                logger.error(f"[data_profiling] {line}")
    if result.returncode != 0:
        raise Exception("Data profiling failed")
    logger.info("Data profiling completed")

@task(name="Preprocess Data")
def preprocess_data():
    logger = get_run_logger()
    logger.info("Preprocessing data...")
    result = subprocess.run(
        ["python", "src/data_processing.py"],
        capture_output=True,
        text=True
    )
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            if line:
                logger.info(f"[data_processing] {line}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            if line:
                logger.error(f"[data_processing] {line}")
    if result.returncode != 0:
        raise Exception("Data preprocessing failed")
    logger.info("Data preprocessing completed")

@task(name="Engineer Features")
def engineer_features():
    logger = get_run_logger()
    logger.info("Engineering features...")
    result = subprocess.run(
        ["python", "src/feature_engineering.py"],
        capture_output=True,
        text=True
    )
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            if line:
                logger.info(f"[feature_engineering] {line}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            if line:
                logger.error(f"[feature_engineering] {line}")
    if result.returncode != 0:
        raise Exception("Feature engineering failed")
    logger.info("Features engineered")

@task(name="Create Feature Store")
def create_feature_store():
    logger = get_run_logger()
    logger.info("Creating versioned feature store...")
    result = subprocess.run(
        ["python", "src/feature_store_creation.py"],
        capture_output=True,
        text=True
    )
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            if line:
                logger.info(f"[feature_store_creation] {line}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            if line:
                logger.error(f"[feature_store_creation] {line}")
    if result.returncode != 0:
        raise Exception("Feature store creation failed")
    logger.info("Feature store created")

@task(name="Train Model")
def train_model():
    logger = get_run_logger()
    logger.info("Training recommendation model...")
    result = subprocess.run(
        ["python", "src/train_model.py"],
        capture_output=True,
        text=True
    )
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            if line:
                logger.info(f"[train_model] {line}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            if line:
                logger.error(f"[train_model] {line}")
    if result.returncode != 0:
        raise Exception("Model training failed")
    logger.info("Model trained and saved")

# ----------------------------
# Define Flow with Sequential Dependencies
# ----------------------------

@flow(name="Core Recommendation Pipeline", log_prints=True)
def recommendation_pipeline():
    logger = get_run_logger()
    logger.info("============================================================")
    logger.info("Starting Core Recommendation Pipeline (Ingest -> Model)")
    logger.info("============================================================")

    # Ingestion phase
    reviews_task = ingest_reviews.submit()
    purchase_task = ingest_purchase_history.submit()
    popularity_task = ingest_product_popularity.submit()

    # Wait for all ingestion to complete before merging
    merge_task = merge_data.submit(wait_for=[reviews_task, purchase_task, popularity_task])

    # Validation and preprocessing
    validate_task = validate_data.submit()
    profile_task = profile_data.submit()
    preprocess_task = preprocess_data.submit()
    engineer_task = engineer_features.submit()
    feature_store_task = create_feature_store.submit()
    train_task = train_model.submit()

    logger.info("Pipeline completed successfully!")

# ----------------------------
# Run
# ----------------------------

if __name__ == "__main__":
    recommendation_pipeline()