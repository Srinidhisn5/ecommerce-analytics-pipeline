Create a professional SQLite database setup for the e-commerce data. Act as a database architect.

OBJECTIVE: Design a normalized, performant database schema and ETL pipeline.

REQUIREMENTS:

1. SCHEMA DESIGN:
   - Create tables matching the CSV structure
   - Use appropriate data types (INTEGER, REAL, TEXT, DATE)
   - Add PRIMARY KEY constraints
   - Add FOREIGN KEY constraints with ON DELETE rules
   - Create indexes on frequently queried columns
   - Follow 3NF normalization

2. DATA LOADING:
   - Read CSVs using pandas
   - Validate data before insertion
   - Handle NULL values appropriately
   - Use transactions for data integrity
   - Add error handling with rollback
   - Log any data quality issues

3. DATA VALIDATION:
   - Check referential integrity
   - Verify date logic (orders after customer registration)
   - Validate price calculations
   - Ensure no orphaned records

4. PERFORMANCE OPTIMIZATION:
   - Create indexes on:
     * customer_id in orders table
     * product_id in order_items table
     * order_id in order_items table
     * customer_id in reviews table
     * category in products table
   - Use ANALYZE to update statistics

DELIVERABLES:
- schema.sql: Complete DDL statements
- setup_database.py: ETL script with error handling
- Include data quality report (row counts, validation results)

CODE STYLE:
- Use parameterized queries (prevent SQL injection)
- Add docstrings
- Include logging
- Make it production-ready

BONUS: Add a function to generate a data quality report showing:
- Row count per table
- Foreign key validation results
- Date range summaries
- Any data anomalies found