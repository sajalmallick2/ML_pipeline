import csv
import json
import os
from datetime import datetime
from collections import defaultdict
import math


def load_metadata(metadata_path):
    """Load product metadata from CSV."""
    print(f"Loading metadata from: {metadata_path}")

    # Structure: reviewer_id, product_id (asin), rating, timestamp
    product_data = defaultdict(lambda: {'ratings': [], 'review_count': 0})

    with open(metadata_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 3:
                product_id = row[1]  # ASIN
                try:
                    rating = float(row[2])
                    product_data[product_id]['ratings'].append(rating)
                    product_data[product_id]['review_count'] += 1
                except ValueError:
                    continue

    print(f"Loaded data for {len(product_data)} products")
    return product_data


def calculate_popularity_score(product_data):
    """
    Calculate popularity score using multiple signals:
    - Average rating (weighted)
    - Review count (log-scaled)
    - Normalized composite score
    """
    print("\nCalculating popularity scores...")

    popularity_data = []

    # Find max review count for normalization
    max_reviews = max([data['review_count'] for data in product_data.values()])

    for product_id, data in product_data.items():
        ratings = data['ratings']
        review_count = data['review_count']

        # Calculate average rating
        avg_rating = sum(ratings) / len(ratings) if ratings else 0

        # Calculate review count score (log-scaled)
        review_score = math.log10(review_count + 1) / \
            math.log10(max_reviews + 1)

        # Composite popularity score
        # Formula: (avg_rating / 5.0) * 0.4 + review_score * 0.6
        # Gives more weight to review count as it indicates engagement
        popularity_score = (avg_rating / 5.0) * 0.4 + review_score * 0.6

        # Scale to 0-100
        popularity_score = round(popularity_score * 100, 2)

        popularity_data.append({
            'product_id': product_id,
            'popularity_score': popularity_score,
            'avg_rating': round(avg_rating, 2),
            'review_count': review_count,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    # Sort by popularity score (descending)
    popularity_data.sort(key=lambda x: x['popularity_score'], reverse=True)

    return popularity_data


def generate_external_api_data(metadata_path, output_path):
    """Generate external API data with product popularity scores."""

    # Load metadata
    product_data = load_metadata(metadata_path)

    # Calculate popularity scores
    popularity_data = calculate_popularity_score(product_data)

    # Write to JSON file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump({
            'metadata': {
                'total_products': len(popularity_data),
                'generated_at': datetime.now().isoformat(),
                'source': 'Amazon Electronics Reviews',
                'popularity_algorithm': 'Composite: (avg_rating/5 * 0.4) + (log_review_count * 0.6) * 100'
            },
            'products': popularity_data
        }, f, indent=2)

    print(
        f"\nCreated product_popularity.json with {len(popularity_data)} products")
    print(f"Saved to: {output_path}")

    # Print statistics
    scores = [p['popularity_score'] for p in popularity_data]
    print("\nPopularity Score Statistics:")
    print("-" * 60)
    print(f"  Min Score:     {min(scores):.2f}")
    print(f"  Max Score:     {max(scores):.2f}")
    print(f"  Avg Score:     {sum(scores)/len(scores):.2f}")
    print(f"  Median Score:  {sorted(scores)[len(scores)//2]:.2f}")

    # Print top products
    print("\nTop 10 Most Popular Products:")
    print("-" * 100)
    for i, product in enumerate(popularity_data[:10], 1):
        print(f"{i:2d}. Product: {product['product_id']} | "
              f"Popularity: {product['popularity_score']:6.2f} | "
              f"Avg Rating: {product['avg_rating']:.2f} | "
              f"Reviews: {product['review_count']:,}")

    # Print sample of least popular
    print("\nSample of Lower Popularity Products:")
    print("-" * 100)
    for i, product in enumerate(popularity_data[-5:], 1):
        print(f"{i}. Product: {product['product_id']} | "
              f"Popularity: {product['popularity_score']:6.2f} | "
              f"Avg Rating: {product['avg_rating']:.2f} | "
              f"Reviews: {product['review_count']:,}")


def main():
    """Main function to generate external API data."""
    print("=" * 80)
    print("External API Data Generator - Product Popularity")
    print("=" * 80)

    # Set paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    metadata_path = os.path.join(
        script_dir, "data", "raw", "products", "metadata", "ratings_Electronics (1).csv")
    output_path = os.path.join(
        script_dir, "data", "raw", "external_api", "product_popularity.json")

    # Check if metadata file exists
    if not os.path.exists(metadata_path):
        print(f"ERROR: Metadata file not found at {metadata_path}")
        return

    # Generate external API data
    generate_external_api_data(metadata_path, output_path)

    print("\n" + "=" * 80)
    print("External API data generation complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
