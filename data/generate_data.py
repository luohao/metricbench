#!/usr/bin/env python3
"""
Standalone synthetic data generator for experimentation benchmarking.
Generates 5 CSV files simulating an e-commerce platform with A/B tests.

Usage:
    python generate_data.py [--users 5000] [--days 120] [--output /tmp/experimentation-benchmark/csv]
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path


# Distributions
BROWSERS = [("Chrome", 0.65), ("Safari", 0.20), ("Firefox", 0.15)]
COUNTRIES = [("US", 0.50), ("UK", 0.20), ("CA", 0.15), ("AU", 0.15)]
EVENTS = ["Add to Cart", "Cart Loaded", "Wishlist", "Search"]

# Experiment config
EXPERIMENT_ID = "checkout-layout"
VARIATIONS = ["0", "1", "2"]
VARIATION_WEIGHTS = [0.34, 0.33, 0.33]

# Simulation baseline date
BASE_DATE = datetime(2021, 10, 1)


def weighted_choice(options):
    """Pick from [(value, weight), ...] list."""
    values, weights = zip(*options)
    return random.choices(values, weights=weights, k=1)[0]


def generate_user_id(index):
    """Generate a short deterministic user ID."""
    return f"u{index:06d}"


def generate_anonymous_id():
    """Generate a random anonymous ID."""
    return uuid.uuid4().hex[:16]


def random_timestamp(day_offset, base_date=BASE_DATE):
    """Generate a random timestamp on a given day."""
    day = base_date + timedelta(days=day_offset)
    seconds = random.randint(0, 86399)
    return day + timedelta(seconds=seconds)


class User:
    def __init__(self, index, run_length_days):
        self.user_id = generate_user_id(index)
        self.anonymous_id = generate_anonymous_id()
        self.browser = weighted_choice(BROWSERS)
        self.country = weighted_choice(COUNTRIES)
        self.run_length_days = run_length_days

        # User starts visiting on a random day in the first 2/3 of the run
        self.first_day = int((index / 5000) * (run_length_days - 30))
        self.first_day = max(0, min(self.first_day, run_length_days - 10))

        # Assign variation (deterministic based on user_id hash)
        self.variation = random.choices(
            VARIATIONS, weights=VARIATION_WEIGHTS, k=1
        )[0]

        # Activity level: sessions per day (0.1 to 2.0)
        self.activity_rate = random.uniform(0.1, 2.0)

        # Purchase probability per session (0.01 to 0.15)
        self.purchase_rate = random.uniform(0.01, 0.15)

        # Event probability per session
        self.event_rate = random.uniform(0.2, 0.8)


def simulate_user(user, tables):
    """Simulate all activity for one user."""
    exposed = False
    session_count = 0

    for day in range(user.first_day, user.run_length_days):
        # Number of sessions this day (Poisson-ish)
        num_sessions = max(0, int(random.gauss(user.activity_rate, 0.5)))

        for _ in range(num_sessions):
            session_count += 1
            session_id = f"{user.user_id}_s{session_count}"
            ts = random_timestamp(day)

            common = {
                "user_id": user.user_id,
                "anonymous_id": user.anonymous_id,
                "session_id": session_id,
                "browser": user.browser,
                "country": user.country,
            }

            # Page view
            pages_in_session = random.randint(1, 5)
            for p in range(pages_in_session):
                page_ts = ts + timedelta(seconds=p * random.randint(10, 120))
                path = random.choice(
                    ["/", "/products", "/cart", "/checkout", "/about"]
                )
                tables["pages"].append(
                    {**common, "timestamp": page_ts.isoformat(), "path": path}
                )

            # Session record
            duration = random.randint(30, 600)
            tables["sessions"].append(
                {
                    **common,
                    "sessionStart": ts.isoformat(),
                    "pages": pages_in_session,
                    "duration": duration,
                }
            )

            # Experiment exposure (first time or re-exposure)
            if not exposed and day >= user.first_day + 5:
                exposed = True
                tables["exposures"].append(
                    {
                        **common,
                        "timestamp": ts.isoformat(),
                        "experiment_id": EXPERIMENT_ID,
                        "variation_id": user.variation,
                    }
                )
            elif exposed and random.random() < 0.05:
                # Small chance of re-exposure
                tables["exposures"].append(
                    {
                        **common,
                        "timestamp": ts.isoformat(),
                        "experiment_id": EXPERIMENT_ID,
                        "variation_id": user.variation,
                    }
                )

            # Events
            if random.random() < user.event_rate:
                event_name = random.choice(EVENTS)
                event_ts = ts + timedelta(seconds=random.randint(5, 300))
                tables["events"].append(
                    {
                        **common,
                        "timestamp": event_ts.isoformat(),
                        "event": event_name,
                        "value": random.randint(1, 10),
                    }
                )

            # Orders
            if random.random() < user.purchase_rate:
                order_ts = ts + timedelta(seconds=random.randint(60, 600))
                qty = random.choices(
                    [1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3], k=1
                )[0]
                # amount can be NULL sometimes
                if random.random() < 0.9:
                    amount = random.choices(
                        [1, 2, 5, 10, 20, 50, 100],
                        weights=[10, 15, 25, 20, 15, 10, 5],
                        k=1,
                    )[0]
                else:
                    amount = None  # ~10% NULL amounts

                tables["orders"].append(
                    {
                        **common,
                        "timestamp": order_ts.isoformat(),
                        "qty": qty,
                        "amount": amount,
                    }
                )


def write_csv(rows, output_dir, filename, fieldnames):
    """Write rows to a CSV file."""
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows):,} rows to {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic e-commerce data for experimentation benchmarking"
    )
    parser.add_argument(
        "--users", type=int, default=5000, help="Number of users (default: 5000)"
    )
    parser.add_argument(
        "--days", type=int, default=120, help="Simulation length in days (default: 120)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="/tmp/experimentation-benchmark/csv",
        help="Output directory for CSV files",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed (default: 42)"
    )
    args = parser.parse_args()

    random.seed(args.seed)

    Path(args.output).mkdir(parents=True, exist_ok=True)

    tables = {
        "pages": [],
        "sessions": [],
        "exposures": [],
        "events": [],
        "orders": [],
    }

    print(f"Generating data for {args.users:,} users over {args.days} days...")
    for i in range(args.users):
        user = User(i, args.days)
        simulate_user(user, tables)
        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1:,} users...")

    print(f"\nWriting CSV files to {args.output}/")

    write_csv(
        tables["exposures"],
        args.output,
        "exposures.csv",
        [
            "user_id",
            "anonymous_id",
            "session_id",
            "browser",
            "country",
            "timestamp",
            "experiment_id",
            "variation_id",
        ],
    )

    write_csv(
        tables["orders"],
        args.output,
        "orders.csv",
        [
            "user_id",
            "anonymous_id",
            "session_id",
            "browser",
            "country",
            "timestamp",
            "qty",
            "amount",
        ],
    )

    write_csv(
        tables["events"],
        args.output,
        "events.csv",
        [
            "user_id",
            "anonymous_id",
            "session_id",
            "browser",
            "country",
            "timestamp",
            "event",
            "value",
        ],
    )

    write_csv(
        tables["pages"],
        args.output,
        "pages.csv",
        [
            "user_id",
            "anonymous_id",
            "session_id",
            "browser",
            "country",
            "timestamp",
            "path",
        ],
    )

    write_csv(
        tables["sessions"],
        args.output,
        "sessions.csv",
        [
            "user_id",
            "anonymous_id",
            "session_id",
            "browser",
            "country",
            "sessionStart",
            "pages",
            "duration",
        ],
    )

    print("\nData generation complete!")
    print(f"  Exposures: {len(tables['exposures']):,}")
    print(f"  Orders:    {len(tables['orders']):,}")
    print(f"  Events:    {len(tables['events']):,}")
    print(f"  Pages:     {len(tables['pages']):,}")
    print(f"  Sessions:  {len(tables['sessions']):,}")


if __name__ == "__main__":
    main()
