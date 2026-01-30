import csv
import os
import random
from datetime import datetime, timedelta


def load_purchase_history(transactions_path):
    """Load purchase history from CSV."""
    print(f"Loading purchase history from: {transactions_path}")

    transactions = []
    with open(transactions_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            transactions.append(row)

    print(f"Loaded {len(transactions)} transactions")
    return transactions


def generate_clickstream_events(transactions, output_path, sample_size=None):
    """
    Generate synthetic clickstream events based on transactions.

    For each purchase, generate a realistic sequence of events:
    - Multiple product views (current product + browsing)
    - Add to cart
    - Optional wishlist
    - Purchase click
    """
    print("\nGenerating clickstream events...")

    if sample_size:
        transactions = random.sample(
            transactions, min(sample_size, len(transactions)))
        print(f"Using sample of {len(transactions)} transactions")

    events = []
    event_id = 1
    device_types = ['web', 'mobile']
    event_types = ['product_view', 'add_to_cart', 'wishlist', 'purchase_click']

    for txn in transactions:
        user_id = txn['user_id']
        product_id = txn['product_id']
        purchase_timestamp = int(txn['transaction_time'])

        # Convert unix timestamp to datetime
        purchase_time = datetime.fromtimestamp(purchase_timestamp)
        device = random.choice(device_types)

        # Generate events leading up to purchase
        # 1. Initial product view (1-24 hours before purchase)
        view_time = purchase_time - timedelta(hours=random.randint(1, 24))
        events.append({
            'event_id': f"EVT{event_id:010d}",
            'user_id': user_id,
            'product_id': product_id,
            'event_type': 'product_view',
            'event_time': view_time.strftime('%Y-%m-%d %H:%M:%S'),
            'event_timestamp': int(view_time.timestamp()),
            'device_type': device
        })
        event_id += 1

        # 2. Additional product views (browsing behavior - 2-5 views)
        num_views = random.randint(2, 5)
        for _ in range(num_views):
            view_time = purchase_time - \
                timedelta(minutes=random.randint(10, 300))
            # Sometimes view the same product, sometimes browse others
            browsed_product = product_id if random.random(
            ) > 0.3 else f"BROWSE{random.randint(1000, 9999)}"

            events.append({
                'event_id': f"EVT{event_id:010d}",
                'user_id': user_id,
                'product_id': browsed_product,
                'event_type': 'product_view',
                'event_time': view_time.strftime('%Y-%m-%d %H:%M:%S'),
                'event_timestamp': int(view_time.timestamp()),
                'device_type': device
            })
            event_id += 1

        # 3. Add to cart (5-60 minutes before purchase)
        cart_time = purchase_time - timedelta(minutes=random.randint(5, 60))
        events.append({
            'event_id': f"EVT{event_id:010d}",
            'user_id': user_id,
            'product_id': product_id,
            'event_type': 'add_to_cart',
            'event_time': cart_time.strftime('%Y-%m-%d %H:%M:%S'),
            'event_timestamp': int(cart_time.timestamp()),
            'device_type': device
        })
        event_id += 1

        # 4. Optional wishlist (30% chance, before add to cart)
        if random.random() < 0.3:
            wishlist_time = cart_time - \
                timedelta(minutes=random.randint(10, 120))
            events.append({
                'event_id': f"EVT{event_id:010d}",
                'user_id': user_id,
                'product_id': product_id,
                'event_type': 'wishlist',
                'event_time': wishlist_time.strftime('%Y-%m-%d %H:%M:%S'),
                'event_timestamp': int(wishlist_time.timestamp()),
                'device_type': device
            })
            event_id += 1

        # 5. Purchase click (at transaction time)
        events.append({
            'event_id': f"EVT{event_id:010d}",
            'user_id': user_id,
            'product_id': product_id,
            'event_type': 'purchase_click',
            'event_time': purchase_time.strftime('%Y-%m-%d %H:%M:%S'),
            'event_timestamp': purchase_timestamp,
            'device_type': device
        })
        event_id += 1

    # Sort events by timestamp
    events.sort(key=lambda x: x['event_timestamp'])

    # Write to CSV
    fieldnames = ['event_id', 'user_id', 'product_id', 'event_type',
                  'event_time', 'event_timestamp', 'device_type']

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)

    print(f"Created clickstream_events.csv with {len(events)} events")
    print(f"Saved to: {output_path}")

    # Print statistics
    event_counts = {}
    device_counts = {}
    for event in events:
        event_type = event['event_type']
        device = event['device_type']
        event_counts[event_type] = event_counts.get(event_type, 0) + 1
        device_counts[device] = device_counts.get(device, 0) + 1

    print("\nEvent Statistics:")
    print("-" * 60)
    for event_type, count in sorted(event_counts.items()):
        print(f"  {event_type:20s}: {count:,}")

    print("\nDevice Statistics:")
    print("-" * 60)
    for device, count in sorted(device_counts.items()):
        print(f"  {device:20s}: {count:,}")

    # Print sample
    print("\nSample events:")
    print("-" * 100)
    for i, evt in enumerate(events[:5], 1):
        print(f"{i}. {evt['event_id']} | User: {evt['user_id']} | "
              f"Product: {evt['product_id']} | Type: {evt['event_type']:15s} | "
              f"Device: {evt['device_type']:6s} | Time: {evt['event_time']}")


def main():
    """Main function to generate clickstream events."""
    print("=" * 80)
    print("Clickstream Events Generator")
    print("=" * 80)

    # Set paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    transactions_path = os.path.join(
        script_dir, "data", "raw", "transactions", "purchase_history.csv")
    output_path = os.path.join(
        script_dir, "data", "raw", "clickstream", "clickstream_events.csv")

    # Check if transactions file exists
    if not os.path.exists(transactions_path):
        print(f"ERROR: Transactions file not found at {transactions_path}")
        return

    # Load transactions
    transactions = load_purchase_history(transactions_path)

    # Generate clickstream events
    # Use a sample for faster generation (remove sample_size parameter to use all)
    generate_clickstream_events(transactions, output_path, sample_size=100000)

    print("\n" + "=" * 80)
    print("Clickstream events generation complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
