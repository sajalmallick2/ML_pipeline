import kagglehub
import os
import shutil


def get_base_paths():
    """Get the base paths for data storage."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    download_path = os.path.join(script_dir, "data", "raw")

    # Define data structure paths
    paths = {
        'base': download_path,
        'reviews': os.path.join(download_path, "reviews"),
        'clickstream': os.path.join(download_path, "clickstream"),
        'transactions': os.path.join(download_path, "transactions"),
        'products': os.path.join(download_path, "products"),
        'external_api': os.path.join(download_path, "external_api")
    }

    return script_dir, paths


def setup_data_structure(paths):
    """Create the required data directory structure."""
    print("\nSetting up data directory structure...")
    print("=" * 60)

    for folder_name, folder_path in paths.items():
        if folder_name != 'base':
            os.makedirs(folder_path, exist_ok=True)
            print(f"✓ Created: {folder_name}/ → {folder_path}")

    # Create README files for each folder
    readme_content = {
        'reviews': "# Reviews Data\nAmazon Electronics reviews dataset.",
        'clickstream': "# Clickstream Data\nSynthetic event logs will be generated here.",
        'transactions': "# Transaction Data\nTransactional purchase history derived from reviews.",
        'products': "# Product Metadata\nProduct information and metadata.",
        'external_api': "# External API Data\nSimulated popularity and sentiment feed data."
    }

    for folder_name, content in readme_content.items():
        readme_path = os.path.join(paths[folder_name], "README.md")
        if not os.path.exists(readme_path):
            with open(readme_path, 'w') as f:
                f.write(content)

    print("=" * 60)


def download_dataset(dataset_id, dataset_name):
    """
    Download a dataset from Kaggle using kagglehub.

    Args:
        dataset_id: Kaggle dataset identifier (e.g., 'owner/dataset-name')
        dataset_name: Friendly name for logging purposes

    Returns:
        Path to the downloaded dataset
    """
    print(f"\nDownloading {dataset_name}...")
    print("=" * 60)
    path = kagglehub.dataset_download(dataset_id)
    print(f"Downloaded to: {path}")
    return path


def copy_dataset(source_path, target_path, dataset_name):
    """
    Copy dataset files from source to target location.

    Args:
        source_path: Source directory path
        target_path: Target directory path
        dataset_name: Friendly name for logging purposes
    """
    # Remove existing target if it exists
    if os.path.exists(target_path):
        shutil.rmtree(target_path)

    # Copy files
    shutil.copytree(source_path, target_path)
    print(f"{dataset_name} copied to: {target_path}")


def download_reviews_dataset(paths):
    """Download Amazon Electronics Reviews dataset."""
    dataset_id = "shivamparab/amazon-electronics-reviews"
    dataset_name = "Amazon Electronics Reviews"

    # Download dataset
    source_path = download_dataset(dataset_id, dataset_name)

    # Copy to reviews folder
    target_path = os.path.join(paths['reviews'])
    copy_dataset(source_path, target_path, dataset_name)

    # Rename the JSON file to electronics_reviews.json
    source_file = os.path.join(target_path, "Electronics_5.json")
    target_file = os.path.join(target_path, "electronics_reviews.json")
    if os.path.exists(source_file):
        os.rename(source_file, target_file)
        print(f"Renamed to: electronics_reviews.json")

    return target_path


def download_metadata_dataset(paths):
    """Download Amazon Electronics Metadata dataset."""
    dataset_id = "saurav9786/amazon-product-reviews"
    dataset_name = "Amazon Electronics Metadata"

    # Download dataset
    source_path = download_dataset(dataset_id, dataset_name)

    # Copy to products folder
    target_path = os.path.join(paths['products'], "metadata")
    copy_dataset(source_path, target_path, dataset_name)

    return target_path


def main():
    """Main function to orchestrate dataset downloads."""
    print("=" * 60)
    print("Amazon Electronics Dataset Downloader")
    print("=" * 60)

    # Get base paths
    script_dir, paths = get_base_paths()

    # Setup data directory structure
    setup_data_structure(paths)

    # Download reviews dataset (for transactions)
    reviews_path = download_reviews_dataset(paths)

    # Download metadata dataset (for products)
    metadata_path = download_metadata_dataset(paths)

    # Summary
    print("\n" + "=" * 60)
    print("Download Complete!")
    print("=" * 60)
    print("\nData Structure:")
    print(f"  data/raw/")
    print(f"    ├── reviews/")
    print(f"    │   └── electronics_reviews.json")
    print(f"    ├── transactions/")
    print(f"    │   └── purchase_history.csv (to be derived)")
    print(f"    ├── clickstream/        [Ready for synthetic event logs]")
    print(
        f"    ├── products/           [Metadata: {os.path.basename(metadata_path)}]")
    print(f"    └── external_api/       [Ready for simulated data]")
    print("\nNext Steps:")
    print("  1. Derive purchase_history.csv from reviews → transactions/")
    print("  2. Generate synthetic clickstream logs → clickstream/")
    print("  3. Simulate external API data → external_api/")
    print("=" * 60)


if __name__ == "__main__":
    main()
