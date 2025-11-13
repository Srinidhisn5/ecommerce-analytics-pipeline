#!/usr/bin/env python3
"""
E-Commerce Database Setup and ETL Pipeline

This script:
- Creates SQLite database schema
- Loads CSV data with validation
- Performs data quality checks
- Generates data quality reports
"""

import argparse
import logging
import sqlite3
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ============================================================================
# Configuration
# ============================================================================

DEFAULT_DB_PATH = Path("database") / "ecommerce.db"
DEFAULT_CSV_DIR = Path("data") / "synthetic"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("database/setup.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ============================================================================
# Database Operations
# ============================================================================


class DatabaseSetup:
    """Handles database creation, schema loading, and data import."""

    def __init__(self, db_path: Path, csv_dir: Path):
        self.db_path = db_path
        self.csv_dir = csv_dir
        self.conn: Optional[sqlite3.Connection] = None
        self.validation_errors: List[str] = []
        self.validation_warnings: List[str] = []

    def connect(self) -> sqlite3.Connection:
        """Create database connection with foreign key support."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        self.conn = conn
        logger.info(f"Connected to database: {self.db_path}")
        return conn

    def create_schema(self, schema_file: Path) -> None:
        """Execute schema SQL file to create tables and indexes."""
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        logger.info(f"Loading schema from {schema_file}")
        with open(schema_file, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        # Remove ANALYZE statement (we'll run it after data load)
        schema_sql = schema_sql.replace("ANALYZE;", "").replace("ANALYZE", "")
        
        # Use executescript for multi-statement SQL
        # This handles semicolon-separated statements properly
        try:
            self.conn.executescript(schema_sql)
            self.conn.commit()
        except sqlite3.Error as e:
            # If tables already exist, that's okay
            error_msg = str(e).lower()
            if "already exists" not in error_msg:
                logger.warning(f"Schema execution warning: {e}")
                # Try to continue anyway
                self.conn.rollback()
        
        # Ensure foreign keys are enabled
        self.conn.execute("PRAGMA foreign_keys = ON")
        
        # Verify tables were created
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Schema created successfully. Tables: {', '.join(tables)}")

    def validate_dataframe(self, df: pd.DataFrame, table_name: str, required_columns: List[str]) -> bool:
        """Validate DataFrame structure and basic data quality."""
        errors = []
        warnings = []

        # Check required columns
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            errors.append(f"{table_name}: Missing required columns: {missing_cols}")

        # Check for empty DataFrame
        if df.empty:
            warnings.append(f"{table_name}: DataFrame is empty")

        # Check for duplicate primary keys
        if table_name == "products" and "product_id" in df.columns:
            duplicates = df[df.duplicated(subset=["product_id"], keep=False)]
            if not duplicates.empty:
                errors.append(f"{table_name}: Duplicate product_id found: {duplicates['product_id'].tolist()[:5]}")

        if table_name == "customers" and "customer_id" in df.columns:
            duplicates = df[df.duplicated(subset=["customer_id"], keep=False)]
            if not duplicates.empty:
                errors.append(f"{table_name}: Duplicate customer_id found: {duplicates['customer_id'].tolist()[:5]}")

        if table_name == "orders" and "order_id" in df.columns:
            duplicates = df[df.duplicated(subset=["order_id"], keep=False)]
            if not duplicates.empty:
                errors.append(f"{table_name}: Duplicate order_id found: {duplicates['order_id'].tolist()[:5]}")

        # Check for NULLs in required fields
        for col in required_columns:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    errors.append(f"{table_name}: {null_count} NULL values in required column '{col}'")

        self.validation_errors.extend(errors)
        self.validation_warnings.extend(warnings)

        if errors:
            for error in errors:
                logger.error(error)
            return False

        if warnings:
            for warning in warnings:
                logger.warning(warning)

        return True

    def load_csv(self, csv_file: Path, table_name: str, required_columns: List[str]) -> pd.DataFrame:
        """Load and validate CSV file."""
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        logger.info(f"Loading {csv_file.name}...")
        df = pd.read_csv(csv_file)

        # Convert date columns
        date_columns = {
            "products": ["created_date"],
            "customers": ["registration_date"],
            "orders": ["order_date"],
            "reviews": ["review_date"],
        }
        if table_name in date_columns:
            for col in date_columns[table_name]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        # Validate DataFrame
        if not self.validate_dataframe(df, table_name, required_columns):
            raise ValueError(f"Data validation failed for {table_name}")

        logger.info(f"Loaded {len(df)} rows from {csv_file.name}")
        return df

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, batch_size: int = 1000) -> int:
        """Insert DataFrame into database using parameterized queries."""
        if df.empty:
            logger.warning(f"Skipping empty DataFrame for {table_name}")
            return 0

        # Prepare column names and placeholders
        columns = [col for col in df.columns if col in df.columns]
        placeholders = ",".join(["?" for _ in columns])
        columns_str = ",".join(columns)

        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        cursor = self.conn.cursor()
        rows_inserted = 0

        try:
            # Convert DataFrame to list of tuples, handling NULLs
            data = []
            for _, row in df.iterrows():
                row_data = []
                for col in columns:
                    value = row[col]
                    # Convert pandas NaT/NaN to None
                    if pd.isna(value):
                        row_data.append(None)
                    elif isinstance(value, date):
                        row_data.append(value.isoformat())
                    else:
                        row_data.append(value)
                data.append(tuple(row_data))

            # Insert in batches
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                cursor.executemany(insert_sql, batch)
                rows_inserted += len(batch)

            self.conn.commit()
            logger.info(f"Inserted {rows_inserted} rows into {table_name}")

        except sqlite3.IntegrityError as e:
            self.conn.rollback()
            logger.error(f"Integrity error inserting into {table_name}: {e}")
            raise
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"Database error inserting into {table_name}: {e}")
            raise

        return rows_inserted

    def validate_referential_integrity(self) -> Dict[str, bool]:
        """Validate foreign key relationships."""
        logger.info("Validating referential integrity...")
        results = {}

        # Check orders -> customers
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*) as invalid_count
            FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
            """
        )
        invalid_orders = cursor.fetchone()[0]
        results["orders_customers"] = invalid_orders == 0
        if invalid_orders > 0:
            self.validation_errors.append(f"Found {invalid_orders} orders with invalid customer_id")

        # Check order_items -> orders
        cursor.execute(
            """
            SELECT COUNT(*) as invalid_count
            FROM order_items oi
            LEFT JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_id IS NULL
            """
        )
        invalid_items = cursor.fetchone()[0]
        results["order_items_orders"] = invalid_items == 0
        if invalid_items > 0:
            self.validation_errors.append(f"Found {invalid_items} order_items with invalid order_id")

        # Check order_items -> products
        cursor.execute(
            """
            SELECT COUNT(*) as invalid_count
            FROM order_items oi
            LEFT JOIN products p ON oi.product_id = p.product_id
            WHERE p.product_id IS NULL
            """
        )
        invalid_products = cursor.fetchone()[0]
        results["order_items_products"] = invalid_products == 0
        if invalid_products > 0:
            self.validation_errors.append(f"Found {invalid_products} order_items with invalid product_id")

        # Check reviews -> products
        cursor.execute(
            """
            SELECT COUNT(*) as invalid_count
            FROM reviews r
            LEFT JOIN products p ON r.product_id = p.product_id
            WHERE p.product_id IS NULL
            """
        )
        invalid_review_products = cursor.fetchone()[0]
        results["reviews_products"] = invalid_review_products == 0
        if invalid_review_products > 0:
            self.validation_errors.append(f"Found {invalid_review_products} reviews with invalid product_id")

        # Check reviews -> customers
        cursor.execute(
            """
            SELECT COUNT(*) as invalid_count
            FROM reviews r
            LEFT JOIN customers c ON r.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
            """
        )
        invalid_review_customers = cursor.fetchone()[0]
        results["reviews_customers"] = invalid_review_customers == 0
        if invalid_review_customers > 0:
            self.validation_errors.append(f"Found {invalid_review_customers} reviews with invalid customer_id")

        return results

    def validate_business_rules(self) -> Dict[str, bool]:
        """Validate business logic constraints."""
        logger.info("Validating business rules...")
        results = {}
        cursor = self.conn.cursor()

        # Check: Order date >= Customer registration date
        cursor.execute(
            """
            SELECT COUNT(*) as invalid_count
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            WHERE o.order_date < c.registration_date
            """
        )
        invalid_dates = cursor.fetchone()[0]
        results["order_date_after_registration"] = invalid_dates == 0
        if invalid_dates > 0:
            self.validation_errors.append(f"Found {invalid_dates} orders with order_date before customer registration_date")

        # Check: Review date >= Order date (for customers who bought the product)
        cursor.execute(
            """
            SELECT COUNT(*) as invalid_count
            FROM reviews r
            JOIN order_items oi ON r.product_id = oi.product_id AND r.customer_id = (
                SELECT customer_id FROM orders WHERE order_id = oi.order_id
            )
            JOIN orders o ON oi.order_id = o.order_id
            WHERE r.review_date < o.order_date
            """
        )
        invalid_review_dates = cursor.fetchone()[0]
        results["review_date_after_order"] = invalid_review_dates == 0
        if invalid_review_dates > 0:
            self.validation_warnings.append(f"Found {invalid_review_dates} reviews with review_date before order_date (may be valid for pre-orders)")

        # Check: Profit margins (20-50%)
        cursor.execute(
            """
            SELECT COUNT(*) as invalid_count
            FROM products
            WHERE (price - cost) / price < 0.20 OR (price - cost) / price > 0.50
            """
        )
        invalid_margins = cursor.fetchone()[0]
        results["profit_margins"] = invalid_margins == 0
        if invalid_margins > 0:
            self.validation_errors.append(f"Found {invalid_margins} products with profit margin outside 20-50% range")

        # Check: Line total calculation
        cursor.execute(
            """
            SELECT COUNT(*) as invalid_count
            FROM order_items
            WHERE ABS(line_total - (quantity * unit_price * (1 - discount))) > 0.01
            """
        )
        invalid_calculations = cursor.fetchone()[0]
        results["line_total_calculation"] = invalid_calculations == 0
        if invalid_calculations > 0:
            self.validation_errors.append(f"Found {invalid_calculations} order_items with incorrect line_total calculation")

        return results

    def generate_data_quality_report(self) -> Dict:
        """Generate comprehensive data quality report."""
        logger.info("Generating data quality report...")
        report = {
            "generated_at": datetime.now().isoformat(),
            "database_path": str(self.db_path),
            "table_counts": {},
            "date_ranges": {},
            "referential_integrity": {},
            "business_rules": {},
            "anomalies": [],
        }

        cursor = self.conn.cursor()

        # Table row counts
        tables = ["products", "customers", "orders", "order_items", "reviews"]
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            report["table_counts"][table] = count

        # Date ranges
        cursor.execute("SELECT MIN(created_date), MAX(created_date) FROM products")
        result = cursor.fetchone()
        report["date_ranges"]["products_created"] = {"min": result[0], "max": result[1]}

        cursor.execute("SELECT MIN(registration_date), MAX(registration_date) FROM customers")
        result = cursor.fetchone()
        report["date_ranges"]["customer_registration"] = {"min": result[0], "max": result[1]}

        cursor.execute("SELECT MIN(order_date), MAX(order_date) FROM orders")
        result = cursor.fetchone()
        report["date_ranges"]["orders"] = {"min": result[0], "max": result[1]}

        cursor.execute("SELECT MIN(review_date), MAX(review_date) FROM reviews")
        result = cursor.fetchone()
        report["date_ranges"]["reviews"] = {"min": result[0], "max": result[1]}

        # Referential integrity
        report["referential_integrity"] = self.validate_referential_integrity()

        # Business rules
        report["business_rules"] = self.validate_business_rules()

        # Anomalies
        anomalies = []

        # Check for orphaned records
        cursor.execute(
            """
            SELECT COUNT(*) FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
            """
        )
        orphaned_orders = cursor.fetchone()[0]
        if orphaned_orders > 0:
            anomalies.append(f"Found {orphaned_orders} orders without matching customers")

        # Check for products with no sales
        cursor.execute(
            """
            SELECT COUNT(*) FROM products p
            LEFT JOIN order_items oi ON p.product_id = oi.product_id
            WHERE oi.product_id IS NULL
            """
        )
        unsold_products = cursor.fetchone()[0]
        if unsold_products > 0:
            anomalies.append(f"Found {unsold_products} products with no sales")

        # Check for customers with no orders
        cursor.execute(
            """
            SELECT COUNT(*) FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            WHERE o.customer_id IS NULL
            """
        )
        inactive_customers = cursor.fetchone()[0]
        if inactive_customers > 0:
            anomalies.append(f"Found {inactive_customers} customers with no orders")

        # Check for orders with no items
        cursor.execute(
            """
            SELECT COUNT(*) FROM orders o
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            WHERE oi.order_id IS NULL
            """
        )
        empty_orders = cursor.fetchone()[0]
        if empty_orders > 0:
            anomalies.append(f"Found {empty_orders} orders with no items")

        report["anomalies"] = anomalies
        report["validation_errors"] = self.validation_errors
        report["validation_warnings"] = self.validation_warnings

        return report

    def print_data_quality_report(self, report: Dict) -> None:
        """Print formatted data quality report."""
        print("\n" + "=" * 80)
        print("DATA QUALITY REPORT")
        print("=" * 80)
        print(f"Generated at: {report['generated_at']}")
        print(f"Database: {report['database_path']}")

        print("\n--- TABLE ROW COUNTS ---")
        for table, count in report["table_counts"].items():
            print(f"  {table:20s}: {count:>8,} rows")

        print("\n--- DATE RANGES ---")
        for key, value in report["date_ranges"].items():
            print(f"  {key:30s}: {value['min']} to {value['max']}")

        print("\n--- REFERENTIAL INTEGRITY ---")
        for check, passed in report["referential_integrity"].items():
            status = "PASS" if passed else "FAIL"
            print(f"  {check:30s}: {status}")

        print("\n--- BUSINESS RULES ---")
        for rule, passed in report["business_rules"].items():
            status = "PASS" if passed else "FAIL"
            print(f"  {rule:30s}: {status}")

        if report["anomalies"]:
            print("\n--- ANOMALIES ---")
            for anomaly in report["anomalies"]:
                print(f"  WARNING: {anomaly}")

        if report["validation_errors"]:
            print("\n--- VALIDATION ERRORS ---")
            for error in report["validation_errors"]:
                print(f"  ERROR: {error}")

        if report["validation_warnings"]:
            print("\n--- VALIDATION WARNINGS ---")
            for warning in report["validation_warnings"]:
                print(f"  WARNING: {warning}")

        print("\n" + "=" * 80)

    def run_etl(self, schema_file: Path) -> None:
        """Execute complete ETL pipeline."""
        try:
            # Connect to database
            self.connect()

            # Create schema
            self.create_schema(schema_file)

            # Define table load order (respecting foreign key dependencies)
            load_sequence = [
                ("products.csv", "products", ["product_id", "name", "category", "subcategory", "price", "cost", "stock_quantity", "supplier", "created_date"]),
                ("customers.csv", "customers", ["customer_id", "first_name", "last_name", "email", "address", "city", "state", "zip", "country", "registration_date"]),
                ("orders.csv", "orders", ["order_id", "customer_id", "order_date", "status", "payment_method", "shipping_address", "shipping_city", "shipping_state", "shipping_zip", "shipping_country", "total_amount"]),
                ("order_items.csv", "order_items", ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount", "line_total"]),
                ("reviews.csv", "reviews", ["review_id", "product_id", "customer_id", "rating", "review_text", "review_date"]),
            ]

            # Reset tables prior to reload using FK-safe order
            self._reset_tables_for_reload()

            # Load data in sequence
            for csv_file, table_name, required_columns in load_sequence:
                csv_path = self.csv_dir / csv_file
                df = self.load_csv(csv_path, table_name, required_columns)

                # Insert data
                self.insert_dataframe(df, table_name)

            # Update statistics
            logger.info("Updating database statistics...")
            self.conn.execute("ANALYZE")
            self.conn.commit()

            # Generate and print report
            report = self.generate_data_quality_report()
            self.print_data_quality_report(report)

            logger.info("ETL pipeline completed successfully")

        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            raise

        finally:
            if self.conn:
                self.conn.close()
                logger.info("Database connection closed")

    def _reset_tables_for_reload(self) -> None:
        """Delete data in FK-safe order inside a single transaction."""
        logger.info("Clearing existing data prior to reload...")
        cursor = self.conn.cursor()
        delete_order = ["order_items", "reviews", "orders", "products", "customers"]
        try:
            cursor.execute("BEGIN")
            for table in delete_order:
                cursor.execute(f"DELETE FROM {table}")
            cursor.execute("COMMIT")
        except sqlite3.Error as exc:
            cursor.execute("ROLLBACK")
            logger.error("Failed to clear existing data; rolled back transaction.")
            raise


# ============================================================================
# Main Entry Point
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="Setup SQLite database and load CSV data")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="Path to SQLite database file")
    parser.add_argument("--csv-dir", type=Path, default=DEFAULT_CSV_DIR, help="Directory containing CSV files")
    parser.add_argument("--schema", type=Path, default=Path("database") / "schema.sql", help="Path to schema SQL file")
    args = parser.parse_args()

    # Ensure directories exist
    args.db_path.parent.mkdir(parents=True, exist_ok=True)

    # Run ETL
    setup = DatabaseSetup(args.db_path, args.csv_dir)
    setup.run_etl(args.schema)


if __name__ == "__main__":
    main()

