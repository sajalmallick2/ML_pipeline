import json
import csv
import os
from datetime import datetime
import random


def load_reviews(reviews_path):
    """Load reviews from JSON file."""
    reviews = []
    print(f"Loading reviews from: {reviews_path}")

    with open(reviews_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                review = json.loads(line.strip())
                reviews.append(review)
            except json.JSONDecodeError:
                continue

    print(f"Loaded {len(reviews)} reviews")
    return reviews


def generate_purchase_history(reviews, output_path):
    """Generate purchase history CSV from reviews data."""
    print("\nGenerating purchase history...")

    transactions = []

    for idx, review in enumerate(reviews, start=1):
        transaction = {
            'transaction_id': f"TXN{idx:08d}",
            'user_id': review.get('reviewerID', 'UNKNOWN'),
            'product_id': review.get('asin', 'UNKNOWN'),
            'transaction_date': review.get('reviewTime', 'Unknown'),
            'transaction_time': review.get('unixReviewTime', 0),
            'quantity': random.randint(1, 3),  # Simulate quantity (1-3 items)
            'price': round(random.uniform(10.0, 500.0), 2),  # Simulate price
            'rating': review.get('overall', 0.0),
        }
        transactions.append(transaction)

    # Write to CSV
    fieldnames = ['transaction_id', 'user_id', 'product_id', 'transaction_date',
                  'transaction_time', 'quantity', 'price', 'rating']

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)

    print(
        f"Created purchase_history.csv with {len(transactions)} transactions")
    print(f"Saved to: {output_path}")

    # Print sample
    print("\nSample transactions:")
    print("-" * 100)
    for i, txn in enumerate(transactions[:3], 1):
        print(f"{i}. TXN: {txn['transaction_id']} | User: {txn['user_id']} | "
              f"Product: {txn['product_id']} | Date: {txn['transaction_date']} | "
              f"Qty: {txn['quantity']} | Price: ${txn['price']}")


def main():
    """Main function to generate purchase history."""
    print("=" * 80)
    print("Purchase History Generator")
    print("=" * 80)

    # Set paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    reviews_path = os.path.join(
        script_dir, "data", "raw", "reviews", "electronics_reviews.json")
    output_path = os.path.join(
        script_dir, "data", "raw", "transactions", "purchase_history.csv")

    # Check if reviews file exists
    if not os.path.exists(reviews_path):
        print(f"ERROR: Reviews file not found at {reviews_path}")
        return

    # Load reviews
    reviews = load_reviews(reviews_path)

    # Generate purchase history
    generate_purchase_history(reviews, output_path)

    print("\n" + "=" * 80)
    print("Purchase history generation complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
