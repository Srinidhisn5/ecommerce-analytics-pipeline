# E-Commerce Database Setup

This directory contains the SQLite database schema and ETL pipeline for the e-commerce analytics dataset.

## Files

- **schema.sql**: Complete database schema with tables, constraints, indexes, and views
- **setup_database.py**: ETL script that loads CSV data into the database with validation
- **ecommerce.db**: SQLite database file (generated after running setup)

## Database Schema

### Tables

1. **products** - Product catalog (200 products)
   - Primary key: `product_id`
   - Indexes: category, subcategory, created_date

2. **customers** - Customer master data (1,000 customers)
   - Primary key: `customer_id`
   - Unique constraint: email
   - Indexes: email, registration_date, state, name

3. **orders** - Order headers (3,000 orders)
   - Primary key: `order_id`
   - Foreign key: `customer_id` → customers
   - Indexes: customer_id, order_date, status, customer+date composite

4. **order_items** - Order line items (8,000 items)
   - Primary key: `order_item_id`
   - Foreign keys: `order_id` → orders, `product_id` → products
   - Indexes: order_id, product_id, order+product composite

5. **reviews** - Product reviews (2,500 reviews)
   - Primary key: `review_id`
   - Foreign keys: `product_id` → products, `customer_id` → customers
   - Indexes: product_id, customer_id, rating, product+rating composite, review_date

### Views

- **v_product_sales_summary**: Product sales aggregations
- **v_customer_order_summary**: Customer order statistics
- **v_monthly_sales_summary**: Monthly sales trends

## Usage

### Basic Setup

```bash
# Load data from default location (data/synthetic/)
python database/setup_database.py

# Specify custom paths
python database/setup_database.py \
    --db-path database/ecommerce.db \
    --csv-dir data/synthetic_demo \
    --schema database/schema.sql
```

### Command Line Options

- `--db-path`: Path to SQLite database file (default: `database/ecommerce.db`)
- `--csv-dir`: Directory containing CSV files (default: `data/synthetic/`)
- `--schema`: Path to schema SQL file (default: `database/schema.sql`)

### Expected CSV Files

The script expects the following CSV files in the specified directory:

- `products.csv`
- `customers.csv`
- `orders.csv`
- `order_items.csv`
- `reviews.csv`

## Data Validation

The ETL pipeline performs comprehensive validation:

### Referential Integrity
- All foreign keys are validated
- Orphaned records are detected

### Business Rules
- Order dates must be after customer registration
- Review dates validated against order dates
- Profit margins checked (20-50% range)
- Line total calculations verified

### Data Quality Report

After loading, the script generates a data quality report showing:
- Row counts per table
- Date ranges
- Referential integrity status
- Business rule compliance
- Data anomalies

## Database Features

### Constraints
- Primary keys on all tables
- Foreign keys with appropriate ON DELETE rules
- CHECK constraints for data validation
- UNIQUE constraints where needed

### Indexes
- Strategic indexes on frequently queried columns
- Composite indexes for common query patterns
- Indexes on foreign keys for join performance

### Performance
- ANALYZE run after data load to update statistics
- Optimized for analytics queries
- Views for common aggregations

## Example Queries

```sql
-- Top selling products
SELECT * FROM v_product_sales_summary 
ORDER BY total_revenue DESC 
LIMIT 10;

-- Customer lifetime value
SELECT * FROM v_customer_order_summary 
ORDER BY total_spent DESC 
LIMIT 10;

-- Monthly sales trends
SELECT * FROM v_monthly_sales_summary;

-- Product reviews summary
SELECT 
    p.name,
    COUNT(r.review_id) as review_count,
    AVG(r.rating) as avg_rating
FROM products p
LEFT JOIN reviews r ON p.product_id = r.product_id
GROUP BY p.product_id, p.name
ORDER BY review_count DESC;
```

## Logging

The setup script logs to:
- Console (INFO level)
- `database/setup.log` file

Check the log file for detailed execution information and any warnings.

## Notes

- The database uses SQLite with foreign key constraints enabled
- All data is loaded in transactions for integrity
- The schema follows 3NF normalization principles
- Views are provided for common analytics use cases

