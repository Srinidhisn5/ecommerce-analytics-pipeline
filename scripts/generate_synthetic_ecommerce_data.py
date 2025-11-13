#!/usr/bin/env python3
"""
Synthetic e-commerce dataset generator.

Generates interconnected CSV files with realistic business rules for analytics use cases.
"""
from __future__ import annotations

import argparse
import math
import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm


# -----------------------------
# Configuration
# -----------------------------

DEFAULT_OUTPUT_DIR = Path("data") / "synthetic"
RANDOM_SEED = 20241113


@dataclass(frozen=True)
class GenerationConfig:
    """Container for high-level generation targets."""

    products: int = 200
    customers: int = 1_000
    orders: int = 3_000
    order_items: int = 8_000
    reviews: int = 2_500


CONFIG = GenerationConfig()


# Category -> (subcategories, (min_price, max_price))
CATEGORY_DEFINITION: Dict[str, Tuple[Sequence[str], Tuple[float, float]]] = {
    "Electronics": (
        ["Smartphones", "Laptops", "Audio", "Gaming", "Wearables"],
        (79.0, 2_500.0),
    ),
    "Clothing": (
        ["Men", "Women", "Kids", "Accessories"],
        (15.0, 300.0),
    ),
    "Home & Garden": (
        ["Furniture", "Kitchen", "Outdoor", "Decor"],
        (25.0, 1_000.0),
    ),
    "Sports": (
        ["Fitness", "Outdoor", "Team Sports", "Cycling"],
        (20.0, 800.0),
    ),
    "Books": (
        ["Fiction", "Non-fiction", "Children", "Academic"],
        (8.0, 120.0),
    ),
}

DISCOUNT_OPTIONS = [0.0, 0.10, 0.20, 0.25]
DISCOUNT_WEIGHTS = [0.70, 0.20, 0.08, 0.02]

ORDER_STATUS_OPTIONS = ["Completed", "Processing", "Cancelled", "Returned"]
ORDER_STATUS_WEIGHTS = [0.80, 0.10, 0.05, 0.05]

PAYMENT_METHOD_OPTIONS = ["Credit Card", "PayPal", "Debit"]
PAYMENT_METHOD_WEIGHTS = [0.60, 0.25, 0.15]

WHALe_SHARE = 0.20
WHALe_REVENUE_TARGET = 0.60
WHALe_ORDER_WEIGHT = 7  # heavily bias whale order selection

SEASONAL_MONTH_WEIGHTS = {
    # Higher weights for Nov/Dec to simulate peak seasonality
    1: 0.85,
    2: 0.80,
    3: 0.90,
    4: 0.95,
    5: 1.00,
    6: 1.05,
    7: 1.10,
    8: 1.05,
    9: 1.10,
    10: 1.20,
    11: 1.60,
    12: 1.65,
}

ORDER_END_DATE = date(2024, 12, 31)


# -----------------------------
# Helper functions
# -----------------------------

def set_random_seed(seed: int = RANDOM_SEED) -> None:
    random.seed(seed)
    np.random.seed(seed)


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def weighted_choice(options: Sequence, weights: Sequence[float]):
    return random.choices(options, weights=weights, k=1)[0]


def random_date_between(start: date, end: date) -> date:
    """Return a random date between start and end inclusive."""
    delta = (end - start).days
    offset = random.randint(0, delta)
    return start + timedelta(days=offset)


def generate_registration_date() -> date:
    return random_date_between(date(2023, 1, 1), date(2024, 10, 31))


def generate_product_created_date() -> date:
    return random_date_between(date(2022, 1, 1), date(2024, 10, 31))


def generate_order_date(registration_date: date) -> date:
    """Generate order date >= registration date with seasonal weighting."""
    max_date = ORDER_END_DATE
    if registration_date >= max_date:
        registration_date = max_date - timedelta(days=1)

    while True:
        year = random.choice([2023, 2024])
        month = weighted_choice(list(SEASONAL_MONTH_WEIGHTS.keys()), list(SEASONAL_MONTH_WEIGHTS.values()))
        last_day = (date(year, month, 1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        day = random.randint(1, last_day.day)
        candidate = date(year, month, day)
        if registration_date <= candidate <= max_date:
            return candidate


def build_email(first_name: str, last_name: str, existing_emails: set[str]) -> str:
    domains = ["example.com", "retailmail.com", "shopmail.com", "gmail.com", "outlook.com"]
    base_email = f"{first_name.lower()}.{last_name.lower()}"
    domain = random.choice(domains)
    candidate = f"{base_email}@{domain}"
    suffix = 1
    while candidate in existing_emails:
        candidate = f"{base_email}{suffix}@{domain}"
        suffix += 1
    existing_emails.add(candidate)
    return candidate


def select_product_for_order(
    products: pd.DataFrame,
    is_whale: bool,
    non_whale_weights: np.ndarray,
    whale_weights: np.ndarray,
) -> pd.Series:
    """Weighted random product selection emphasising electronics and high-value items for whales."""
    weights = whale_weights if is_whale else non_whale_weights
    idx = np.random.choice(products.index.values, p=weights)
    return products.loc[idx]


def rating_probabilities(price_percentile: float) -> List[float]:
    """
    Adjust rating distribution based on product price percentile.
    Higher percentile -> better reviews.
    """
    base = np.array([0.40, 0.30, 0.15, 0.10, 0.05])  # ratings 5..1
    shift_strength = (price_percentile - 0.5) * 0.30  # amplify by +/- 15%
    # Positive shift moves probability mass towards higher ratings
    if shift_strength > 0:
        transfer = min(shift_strength, 0.20)
        base[0] += transfer
        base[1] += transfer / 2
        base[3] -= transfer / 2
        base[4] -= transfer
    else:
        transfer = min(abs(shift_strength), 0.20)
        base[0] -= transfer
        base[1] -= transfer / 2
        base[3] += transfer / 2
        base[4] += transfer
    # Ensure no negative probabilities and normalise
    base = np.clip(base, 0.01, None)
    base = base / base.sum()
    return base.tolist()


def rating_text_template(rating: int, product_category: str) -> str:
    positive_templates = {
        "Electronics": [
            "Performance exceeded expectations with seamless setup.",
            "Battery life and build quality are top-notch.",
        ],
        "Clothing": [
            "Fits perfectly and fabric feels premium.",
            "Stylish and comfortable for everyday wear.",
        ],
        "Home & Garden": [
            "Quality craftsmanship and easy assembly.",
            "Adds instant charm to the space.",
        ],
        "Sports": [
            "Great durability during intense workouts.",
            "Lightweight and enhances performance.",
        ],
        "Books": [
            "Engaging read with well-developed characters.",
            "Extremely informative and well structured.",
        ],
    }
    neutral_templates = [
        "Overall solid value though a few minor quirks.",
        "Meets expectations but could use refinements.",
    ]
    negative_templates = [
        "Item quality did not match the description.",
        "Had issues shortly after purchase; disappointed.",
    ]

    if rating >= 4:
        return random.choice(positive_templates.get(product_category, neutral_templates))
    if rating == 3:
        return random.choice(neutral_templates)
    return random.choice(negative_templates)


# -----------------------------
# Generators
# -----------------------------

def generate_products(faker: Faker, config: GenerationConfig) -> pd.DataFrame:
    products_records = []
    for product_id in range(1, config.products + 1):
        category = weighted_choice(
            list(CATEGORY_DEFINITION.keys()),
            [0.30, 0.25, 0.20, 0.15, 0.10],
        )
        subcategories, (min_price, max_price) = CATEGORY_DEFINITION[category]
        subcategory = random.choice(subcategories)
        price = round(random.uniform(min_price, max_price), 2)
        margin = random.uniform(0.20, 0.50)
        cost = round(price * (1 - margin), 2)
        products_records.append(
            {
                "product_id": product_id,
                "name": faker.catch_phrase(),
                "category": category,
                "subcategory": subcategory,
                "price": price,
                "cost": cost,
                "stock_quantity": random.randint(10, 1_000),
                "supplier": faker.company(),
                "created_date": generate_product_created_date(),
            }
        )
    df = pd.DataFrame(products_records)
    return df


def generate_customers(faker: Faker, config: GenerationConfig) -> pd.DataFrame:
    customer_records = []
    unique_emails: set[str] = set()
    for customer_id in range(1, config.customers + 1):
        first_name = faker.first_name()
        last_name = faker.last_name()
        address = faker.street_address()
        city = faker.city()
        state = faker.state_abbr()
        postal_code = faker.zipcode()
        country = "USA"
        registration_date = generate_registration_date()
        customer_records.append(
            {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": build_email(first_name, last_name, unique_emails),
                "phone": faker.numerify("###-###-####"),
                "address": address,
                "city": city,
                "state": state,
                "zip": postal_code,
                "country": country,
                "registration_date": registration_date,
            }
        )
    return pd.DataFrame(customer_records)


def choose_shipping_address(faker: Faker, customer_row: pd.Series) -> Tuple[str, str, str, str, str]:
    """Mostly reuse customer address; occasionally ship to alternate location."""
    if random.random() < 0.85:
        return (
            customer_row["address"],
            customer_row["city"],
            customer_row["state"],
            customer_row["zip"],
            customer_row["country"],
        )
    return (
        faker.street_address(),
        faker.city(),
        faker.state_abbr(),
        faker.zipcode(),
        "USA",
    )


def generate_orders(
    faker: Faker,
    customers: pd.DataFrame,
    config: GenerationConfig,
    whale_ids: set[int],
) -> pd.DataFrame:
    customer_weights = customers["customer_id"].apply(
        lambda cid: WHALe_ORDER_WEIGHT if cid in whale_ids else 1
    )
    probabilities = customer_weights / customer_weights.sum()

    orders = []
    for order_id in tqdm(range(1, config.orders + 1), desc="Generating orders", unit="order"):
        customer_id = int(np.random.choice(customers["customer_id"], p=probabilities))
        customer_row = customers.loc[customers["customer_id"] == customer_id].iloc[0]
        order_date = generate_order_date(customer_row["registration_date"])
        shipping_address, shipping_city, shipping_state, shipping_zip, shipping_country = choose_shipping_address(faker, customer_row)
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date,
                "status": weighted_choice(ORDER_STATUS_OPTIONS, ORDER_STATUS_WEIGHTS),
                "payment_method": weighted_choice(PAYMENT_METHOD_OPTIONS, PAYMENT_METHOD_WEIGHTS),
                "shipping_address": shipping_address,
                "shipping_city": shipping_city,
                "shipping_state": shipping_state,
                "shipping_zip": shipping_zip,
                "shipping_country": shipping_country,
                "total_amount": 0.0,  # placeholder to be populated once items are generated
            }
        )
    return pd.DataFrame(orders)


def generate_order_items(
    orders: pd.DataFrame,
    products: pd.DataFrame,
    config: GenerationConfig,
    whale_ids: set[int],
) -> pd.DataFrame:
    """Generate order items for each order while enforcing target line item count."""
    orders.reset_index(drop=True, inplace=True)

    # Pre-compute items per order
    base_counts = np.random.choice(
        [1, 2, 3, 4, 5],
        size=len(orders),
        p=[0.10, 0.35, 0.30, 0.15, 0.10],
    )

    total_items = base_counts.sum()
    target = config.order_items

    # Adjust counts to reach the exact target
    while total_items < target:
        idx = random.randrange(len(base_counts))
        if base_counts[idx] < 5:
            base_counts[idx] += 1
            total_items += 1
    while total_items > target:
        idx = random.randrange(len(base_counts))
        if base_counts[idx] > 1:
            base_counts[idx] -= 1
            total_items -= 1

    # Precompute selection weights with electronics and price emphasis
    category_weight_map = {
        "Electronics": 2.2,
        "Clothing": 1.0,
        "Home & Garden": 1.2,
        "Sports": 1.1,
        "Books": 0.8,
    }

    category_weights_series = products["category"].map(category_weight_map).astype(float)
    price_percentiles = products["price"].rank(pct=True).values
    non_whale_weights = category_weights_series.values * (0.65 + 0.70 * price_percentiles)
    whale_weights = category_weights_series.values * (0.85 + 1.10 * price_percentiles)
    non_whale_weights = non_whale_weights / non_whale_weights.sum()
    whale_weights = whale_weights / whale_weights.sum()

    order_items_records = []

    item_id = 1
    for order_idx, order_row in tqdm(orders.iterrows(), total=len(orders), desc="Generating order items", unit="order"):
        items_count = base_counts[order_idx]
        is_whale = order_row["customer_id"] in whale_ids

        for _ in range(items_count):
            product = select_product_for_order(products, is_whale, non_whale_weights, whale_weights)
            quantity = int(np.random.choice(
                [1, 2, 3, 4, 5],
                p=[0.45, 0.30, 0.15, 0.07, 0.03],
            ))
            if is_whale:
                quantity = min(quantity + np.random.choice([0, 1], p=[0.7, 0.3]), 5)
            unit_price = round(product["price"] * np.random.uniform(0.95, 1.05), 2)
            discount = np.random.choice(DISCOUNT_OPTIONS, p=DISCOUNT_WEIGHTS)
            line_total = round(quantity * unit_price * (1 - discount), 2)
            order_items_records.append(
                {
                    "order_item_id": item_id,
                    "order_id": order_row["order_id"],
                    "product_id": int(product["product_id"]),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount": discount,
                    "line_total": line_total,
                }
            )
            item_id += 1

    order_items_df = pd.DataFrame(order_items_records)

    # Update order totals
    totals = order_items_df.groupby("order_id")["line_total"].sum()
    orders["total_amount"] = orders["order_id"].map(totals).fillna(0.0)

    return order_items_df


def generate_reviews(
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    products: pd.DataFrame,
    config: GenerationConfig,
) -> pd.DataFrame:
    orders_subset = orders[["order_id", "customer_id", "order_date"]]
    merged = order_items.merge(orders_subset, on="order_id", how="left", validate="many_to_one")
    merged = merged.merge(products[["product_id", "category", "price"]], on="product_id", how="left", validate="many_to_one")

    # Weight selection by revenue to ensure popular products accrue more reviews
    weights = merged["line_total"].values
    weights = np.where(weights <= 0, 0.01, weights)
    weights = weights / weights.sum()
    chosen_indices = np.random.choice(
        len(merged),
        size=config.reviews,
        replace=False,
        p=weights,
    )

    # Pre-compute price percentiles
    product_price_percentiles = products.set_index("product_id")["price"].rank(pct=True)

    reviews_records = []
    for review_id, idx in enumerate(chosen_indices, start=1):
        row = merged.iloc[idx]
        price_pct = float(product_price_percentiles.loc[row["product_id"]])
        rating_probs = rating_probabilities(price_pct)
        rating = random.choices([5, 4, 3, 2, 1], weights=rating_probs, k=1)[0]
        review_date = row["order_date"] + timedelta(days=random.randint(1, 60))
        if review_date > ORDER_END_DATE + timedelta(days=60):
            review_date = ORDER_END_DATE + timedelta(days=random.randint(1, 45))

        reviews_records.append(
            {
                "review_id": review_id,
                "product_id": int(row["product_id"]),
                "customer_id": int(row["customer_id"]),
                "rating": rating,
                "review_text": rating_text_template(rating, row["category"]),
                "review_date": review_date,
            }
        )

    return pd.DataFrame(reviews_records)


# -----------------------------
# Validation
# -----------------------------

def validate_datasets(
    products: pd.DataFrame,
    customers: pd.DataFrame,
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    reviews: pd.DataFrame,
    whale_ids: set[int],
) -> None:
    """Perform basic referential and business rule validations."""
    assert len(products) == CONFIG.products, "Unexpected product count"
    assert len(customers) == CONFIG.customers, "Unexpected customer count"
    assert len(orders) == CONFIG.orders, "Unexpected order count"
    assert len(order_items) == CONFIG.order_items, "Unexpected order item count"
    assert len(reviews) == CONFIG.reviews, "Unexpected review count"

    # Referential integrity
    assert orders["customer_id"].isin(customers["customer_id"]).all(), "Order references unknown customer"
    assert order_items["order_id"].isin(orders["order_id"]).all(), "Order item references unknown order"
    assert order_items["product_id"].isin(products["product_id"]).all(), "Order item references unknown product"
    assert reviews["customer_id"].isin(customers["customer_id"]).all(), "Review references unknown customer"
    assert reviews["product_id"].isin(products["product_id"]).all(), "Review references unknown product"

    # Order date >= registration date
    merged = orders.merge(customers[["customer_id", "registration_date"]], on="customer_id", how="left")
    assert (merged["order_date"] >= merged["registration_date"]).all(), "Order predates customer registration"

    # Profit margin checks
    margins = (products["price"] - products["cost"]) / products["price"]
    assert margins.between(0.20, 0.50).all(), "Product margin outside expected range"

    # Revenue concentration
    revenue_by_order = order_items.groupby("order_id")["line_total"].sum()
    revenue_per_order = revenue_by_order.reindex(orders["order_id"], fill_value=0.0)
    orders_with_revenue = orders.assign(_total=revenue_per_order.values)
    whale_revenue = orders_with_revenue.loc[orders_with_revenue["customer_id"].isin(whale_ids), "_total"].sum()
    total_revenue = orders_with_revenue["_total"].sum()
    if total_revenue > 0:
        whale_share = whale_revenue / total_revenue
        assert whale_share >= WHALe_REVENUE_TARGET - 0.02, f"Whale revenue share too low: {whale_share:.2%}"

    # Electronics order value check
    order_has_electronics = order_items.merge(
        products[["product_id", "category"]],
        on="product_id",
        how="left",
        validate="many_to_one",
    )
    electronics_orders = order_has_electronics.loc[order_has_electronics["category"] == "Electronics", "order_id"].unique()
    non_electronics_orders = set(orders["order_id"]) - set(electronics_orders)

    electronics_avg = orders.loc[orders["order_id"].isin(electronics_orders), "total_amount"].mean()
    other_avg = orders.loc[orders["order_id"].isin(non_electronics_orders), "total_amount"].mean()
    if not math.isnan(electronics_avg) and not math.isnan(other_avg):
        assert electronics_avg >= other_avg, "Electronics orders should have higher average value"

    # Discount distribution sanity check
    discount_counts = order_items["discount"].value_counts(normalize=True)
    for discount, expected in zip(DISCOUNT_OPTIONS, DISCOUNT_WEIGHTS):
        observed = discount_counts.get(discount, 0)
        assert abs(observed - expected) <= 0.05, "Discount distribution deviates more than expected"


# -----------------------------
# Orchestration
# -----------------------------

def export_frames(output_dir: Path, frames: Dict[str, pd.DataFrame]) -> None:
    for name, frame in frames.items():
        frame.to_csv(output_dir / f"{name}.csv", index=False)


def main(output_dir: Path) -> None:
    set_random_seed()
    ensure_output_dir(output_dir)
    faker = Faker("en_US")
    Faker.seed(RANDOM_SEED)

    print("Generating products...")
    products = generate_products(faker, CONFIG)

    print("Generating customers...")
    customers = generate_customers(faker, CONFIG)

    whale_count = int(CONFIG.customers * WHALe_SHARE)
    whale_ids = set(random.sample(list(customers["customer_id"]), whale_count))

    print("Generating orders...")
    orders = generate_orders(faker, customers, CONFIG, whale_ids)

    print("Generating order items...")
    order_items = generate_order_items(orders, products, CONFIG, whale_ids)

    print("Generating reviews...")
    reviews = generate_reviews(orders, order_items, products, CONFIG)

    print("Running data validations...")
    validate_datasets(products, customers, orders, order_items, reviews, whale_ids)

    print(f"Exporting CSVs to {output_dir.resolve()} ...")
    export_frames(
        output_dir,
        {
            "products": products,
            "customers": customers,
            "orders": orders,
            "order_items": order_items,
            "reviews": reviews,
        },
    )

    print("Generation complete [done]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce analytics dataset.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Destination directory for generated CSV files.",
    )
    args = parser.parse_args()
    main(args.output_dir)

