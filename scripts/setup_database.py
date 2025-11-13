#!/usr/bin/env python3
"""
E-Commerce Database Setup and ETL Pipeline (Production-Grade)

This script:
- Creates SQLite database schema
- Loads CSV data with validation
- Performs data quality checks
- Generates a data quality report

Usage:
    python scripts/setup_database.py

Defaults assume CSV files live in `data/synthetic/` and schema in `database/schema.sql`.
"""

import argparse
import logging
import os
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

DEFAULT_DB_PATH = Path("database") / "ecommerce.db"
DEFAULT_CSV_DIR = Path("data") / "synthetic"
DEFAULT_SCHEMA = Path("database") / "schema.sql"
LOG_DIR = Path("database")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "setup.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# DatabaseSetup class
# -----------------------------------------------------------------------------

class DatabaseSetup:
    """Handles database creation, schema loading, and data import."""

    def __init__(self, db_path: Path, csv_dir: Path):
        self.db_path = db_path
        self.csv_dir = csv_dir
        self.conn: Optional[sqlite3.Connection] = None
        self.validation_errors: List[str] = []
        self.validation_warnings: List[str] = []

    # ----------------------------- Connection ---------------------------------
    def connect(self) -> sqlite3.Connection:
        """Create database connection with foreign key support."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        self.conn = conn
        logger.info(f"Connected to database: {self.db_path}")
        return conn

    # ----------------------------- Schema -------------------------------------
    def create_schema(self, schema_file: Path) -> None:
        """Execute schema SQL file to create tables and indexes."""
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        logger.info(f"Loading schema from {schema_file}")
        with open(schema_file, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        # Remove standalone ANALYZE statements (we'll run ANALYZE after loading)
        schema_sql = schema_sql.replace("ANALYZE;", "").replace("ANALYZE", "")

        try:
            self.conn.executescript(schema_sql)
            self.conn.commit()
        except sqlite3.Error as e:
            error_msg = str(e).lower()
            if "already exists" not in error_msg:
                logger.warning(f"Schema execution warning: {e}")
                self.conn.rollback()

        # Ensure foreign keys are enabled
        self.conn.execute("PRAGMA foreign_keys = ON")

        # Verify created tables
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Schema created/verified. Tables: {', '.join(tables)}")

    # -------------------------- Reset tables (FK-safe) ------------------------
    def _reset_tables_for_reload(self) -> None:
        """Delete data in FK-safe order inside a transaction.

        This uses a fixed, explicit order to avoid accidental FK violations.
        """
        logger.info("Clearing existing data prior to reload (FK-safe order)...")
        cursor = self.conn.cursor()
        # FK-safe order: delete children before parents
        delete_order = ["order_items", "reviews", "orders", "products", "customers"]
        try:
            cursor.execute("BEGIN")
            for table in delete_order:
                # only delete if table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
                if cursor.fetchone():
                    cursor.execute(f"DELETE FROM {table}")
                    logger.info(f"Cleared table: {table}")
            cursor.execute("COMMIT")
            logger.info("Data cleared (transaction committed)")
        except sqlite3.Error as exc:
            cursor.execute("ROLLBACK")
            logger.error("Failed to clear existing data; rolled back transaction.")
            raise

    # -------------------------- Validation helpers ---------------------------
    def validate_dataframe(self, df: pd.DataFrame, table_name: str, required_columns: List[str]) -> bool:
        """Validate DataFrame structure and basic data quality."""
        errors: List[str] = []
        warnings: List[str] = []

        # Required columns
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            errors.append(f"{table_name}: Missing required columns: {sorted(list(missing_cols))}")

        # Empty check
        if df.empty:
            warnings.append(f"{table_name}: DataFrame is empty")

        # Duplicate PK checks
        pk_map = {"products": "product_id", "customers": "customer_id", "orders": "order_id"}
        if table_name in pk_map and pk_map[table_name] in df.columns:
            pk = pk_map[table_name]
            dup = df[df.duplicated(subset=[pk], keep=False)]
            if not dup.empty:
                errors.append(f"{table_name}: Duplicate {pk} found (sample): {dup[pk].unique()[:5].tolist()}")

        # NULL checks on required columns
        for col in required_columns:
            if col in df.columns:
                null_count = int(df[col].isna().sum())
                if null_count > 0:
                    errors.append(f"{table_name}: {null_count} NULL values in required column '{col}'")

        # Aggregate errors/warnings
        self.validation_errors.extend(errors)
        self.validation_warnings.extend(warnings)

        for e in errors:
            logger.error(e)
        for w in warnings:
            logger.warning(w)

        return len(errors) == 0

    # ----------------------------- CSV loading -------------------------------
    def load_csv(self, csv_file: Path, table_name: str, required_columns: List[str]) -> pd.DataFrame:
        """Load CSV and run lightweight validation and normalization."""
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        logger.info(f"Loading {csv_file} for table {table_name}...")
        df = pd.read_csv(csv_file)

        # Normalize date columns to ISO date strings
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

        # Run validation
        ok = self.validate_dataframe(df, table_name, required_columns)
        if not ok:
            raise ValueError(f"Data validation failed for {table_name}; check logs for details")

        logger.info(f"Loaded {len(df)} rows from {csv_file.name}")
        return df

    # ----------------------------- Insert data --------------------------------
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, batch_size: int = 1000) -> int:
        """Insert DataFrame into DB using parameterized queries (batches)."""
        if df.empty:
            logger.warning(f"Skipping empty DataFrame for {table_name}")
            return 0

        columns = list(df.columns)
        placeholders = ",".join(["?" for _ in columns])
        columns_str = ",".join(columns)
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        cursor = self.conn.cursor()
        rows_inserted = 0

        try:
            data = []
            for _, row in df.iterrows():
                values = []
                for col in columns:
                    val = row[col]
                    if pd.isna(val):
                        values.append(None)
                    elif isinstance(val, date):
                        values.append(val.isoformat())
                    else:
                        values.append(val)
                data.append(tuple(values))

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

    # ------------------------- Referential & business checks -----------------
    def validate_referential_integrity(self) -> Dict[str, bool]:
        logger.info("Validating referential integrity...")
        results: Dict[str, bool] = {}
        cur = self.conn.cursor()

        checks = {
            "orders_customers": (
                "SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL",
                0,
            ),
            "order_items_orders": (
                "SELECT COUNT(*) FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.order_id WHERE o.order_id IS NULL",
                0,
            ),
            "order_items_products": (
                "SELECT COUNT(*) FROM order_items oi LEFT JOIN products p ON oi.product_id = p.product_id WHERE p.product_id IS NULL",
                0,
            ),
            "reviews_products": (
                "SELECT COUNT(*) FROM reviews r LEFT JOIN products p ON r.product_id = p.product_id WHERE p.product_id IS NULL",
                0,
            ),
            "reviews_customers": (
                "SELECT COUNT(*) FROM reviews r LEFT JOIN customers c ON r.customer_id = c.customer_id WHERE c.customer_id IS NULL",
                0,
            ),
        }

        for name, (sql, expect) in checks.items():
            cur.execute(sql)
            invalid = cur.fetchone()[0]
            results[name] = invalid == expect
            if invalid > 0:
                self.validation_errors.append(f"Found {invalid} invalid rows for {name}")

        return results

    def validate_business_rules(self) -> Dict[str, bool]:
        logger.info("Validating business rules...")
        results: Dict[str, bool] = {}
        cur = self.conn.cursor()

        # Order date after registration
        cur.execute(
            "SELECT COUNT(*) FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE o.order_date < c.registration_date"
        )
        invalid_dates = cur.fetchone()[0]
        results["order_date_after_registration"] = invalid_dates == 0
        if invalid_dates > 0:
            self.validation_errors.append(f"Found {invalid_dates} orders with order_date before registration_date")

        # Profit margin check
        cur.execute(
            "SELECT COUNT(*) FROM products WHERE (price - cost) / price < 0.20 OR (price - cost) / price > 0.50"
        )
        invalid_margin = cur.fetchone()[0]
        results["profit_margins"] = invalid_margin == 0
        if invalid_margin > 0:
            self.validation_errors.append(f"Found {invalid_margin} products with profit margin outside 20-50%")

        # Line total calculation
        cur.execute(
            "SELECT COUNT(*) FROM order_items WHERE ABS(line_total - (quantity * unit_price * (1 - discount))) > 0.01"
        )
        invalid_calc = cur.fetchone()[0]
        results["line_total_calculation"] = invalid_calc == 0
        if invalid_calc > 0:
            self.validation_errors.append(f"Found {invalid_calc} order_items with incorrect line_total calculation")

        return results

    # --------------------------- Data quality report -------------------------
    def generate_data_quality_report(self) -> Dict:
        logger.info("Generating data quality report...")
        report: Dict = {
            "generated_at": datetime.now().isoformat(),
            "database_path": str(self.db_path),
            "table_counts": {},
            "date_ranges": {},
            "referential_integrity": {},
            "business_rules": {},
            "anomalies": [],
            "validation_errors": self.validation_errors,
            "validation_warnings": self.validation_warnings,
        }

        cur = self.conn.cursor()
        tables = ["products", "customers", "orders", "order_items", "reviews"]
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            report["table_counts"][t] = cur.fetchone()[0]

        # Date ranges
        cur.execute("SELECT MIN(created_date), MAX(created_date) FROM products")
        r = cur.fetchone()
        report["date_ranges"]["products_created"] = {"min": r[0], "max": r[1]}

        cur.execute("SELECT MIN(registration_date), MAX(registration_date) FROM customers")
        r = cur.fetchone()
        report["date_ranges"]["customer_registration"] = {"min": r[0], "max": r[1]}

        cur.execute("SELECT MIN(order_date), MAX(order_date) FROM orders")
        r = cur.fetchone()
        report["date_ranges"]["orders"] = {"min": r[0], "max": r[1]}

        cur.execute("SELECT MIN(review_date), MAX(review_date) FROM reviews")
        r = cur.fetchone()
        report["date_ranges"]["reviews"] = {"min": r[0], "max": r[1]}

        # Referential & business checks
        report["referential_integrity"] = self.validate_referential_integrity()
        report["business_rules"] = self.validate_business_rules()

        # Anomalies
        cur.execute(
            "SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL"
        )
        orphaned = cur.fetchone()[0]
        if orphaned > 0:
            report["anomalies"].append(f"Found {orphaned} orders without matching customers")

        cur.execute(
            "SELECT COUNT(*) FROM products p LEFT JOIN order_items oi ON p.product_id = oi.product_id WHERE oi.product_id IS NULL"
        )
        unsold = cur.fetchone()[0]
        if unsold > 0:
            report["anomalies"].append(f"Found {unsold} products with no sales")

        cur.execute(
            "SELECT COUNT(*) FROM orders o LEFT JOIN order_items oi ON o.order_id = oi.order_id WHERE oi.order_id IS NULL"
        )
        empty_orders = cur.fetchone()[0]
        if empty_orders > 0:
            report["anomalies"].append(f"Found {empty_orders} orders with no items")

        return report

    def print_data_quality_report(self, report: Dict) -> None:
        print("\n" + "=" * 80)
        print("DATA QUALITY REPORT")
        print("=" * 80)
        print(f"Generated at: {report['generated_at']}")
        print(f"Database: {report['database_path']}")

        print("\n--- TABLE ROW COUNTS ---")
        for table, count in report["table_counts"].items():
            print(f"  {table:20s}: {count:>8,} rows")

        print("\n--- DATE RANGES ---")
        for key, val in report["date_ranges"].items():
            print(f"  {key:30s}: {val['min']} to {val['max']}")

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
            for a in report["anomalies"]:
                print(f"  WARNING: {a}")

        if report["validation_errors"]:
            print("\n--- VALIDATION ERRORS ---")
            for e in report["validation_errors"]:
                print(f"  ERROR: {e}")

        if report["validation_warnings"]:
            print("\n--- VALIDATION WARNINGS ---")
            for w in report["validation_warnings"]:
                print(f"  WARNING: {w}")

        print("\n" + "=" * 80)


# -----------------------------------------------------------------------------
# Main ETL flow
# -----------------------------------------------------------------------------

    def run_etl(self, schema_file: Path) -> None:
        """Execute complete ETL pipeline."""
        try:
            # Connect to database
            self.connect()

            # Create schema
            self.create_schema(schema_file)

            # Clear all tables ONCE before loading
            self._reset_tables_for_reload()

            # Define table load order (respecting foreign key dependencies)
            load_sequence = [
                ("products.csv", "products", ["product_id", "name", "category", "subcategory", "price", "cost", "stock_quantity", "supplier", "created_date"]),
                ("customers.csv", "customers", ["customer_id", "first_name", "last_name", "email", "address", "city", "state", "zip", "country", "registration_date"]),
                ("orders.csv", "orders", ["order_id", "customer_id", "order_date", "status", "payment_method", "shipping_address", "shipping_city", "shipping_state", "shipping_zip", "shipping_country", "total_amount"]),
                ("order_items.csv", "order_items", ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount", "line_total"]),
                ("reviews.csv", "reviews", ["review_id", "product_id", "customer_id", "rating", "review_text", "review_date"]),
            ]

            # Load data in sequence
            for csv_file, table_name, required_columns in load_sequence:
                csv_path = self.csv_dir / csv_file
                df = self.load_csv(csv_path, table_name, required_columns)
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


# ============================================================================
# Main Entry Point
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="Setup SQLite database and load CSV data")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="Path to SQLite database file")
    parser.add_argument("--csv-dir", type=Path, default=DEFAULT_CSV_DIR, help="Directory containing CSV files")
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA, help="Path to schema SQL file")
    args = parser.parse_args()

    # Ensure directories exist
    args.db_path.parent.mkdir(parents=True, exist_ok=True)

    # Run ETL
    setup = DatabaseSetup(args.db_path, args.csv_dir)
    setup.run_etl(args.schema)


if __name__ == "__main__":
    main()
