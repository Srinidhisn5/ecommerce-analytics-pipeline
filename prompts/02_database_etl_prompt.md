Create a professional SQLite database setup for the e-commerce data. Act as a database architect.



OBJECTIVE: Design a normalized, performant database schema and ETL pipeline.



REQUIREMENTS:



1\. SCHEMA DESIGN:

&nbsp;  - Create tables matching the CSV structure

&nbsp;  - Use appropriate data types (INTEGER, REAL, TEXT, DATE)

&nbsp;  - Add PRIMARY KEY constraints

&nbsp;  - Add FOREIGN KEY constraints with ON DELETE rules

&nbsp;  - Create indexes on frequently queried columns

&nbsp;  - Follow 3NF normalization



2\. DATA LOADING:

&nbsp;  - Read CSVs using pandas

&nbsp;  - Validate data before insertion

&nbsp;  - Handle NULL values appropriately

&nbsp;  - Use transactions for data integrity

&nbsp;  - Add error handling with rollback

&nbsp;  - Log any data quality issues



3\. DATA VALIDATION:

&nbsp;  - Check referential integrity

&nbsp;  - Verify date logic (orders after customer registration)

&nbsp;  - Validate price calculations

&nbsp;  - Ensure no orphaned records



4\. PERFORMANCE OPTIMIZATION:

&nbsp;  - Create indexes on:

&nbsp;    \* customer\_id in orders table

&nbsp;    \* product\_id in order\_items table

&nbsp;    \* order\_id in order\_items table

&nbsp;    \* customer\_id in reviews table

&nbsp;    \* category in products table

&nbsp;  - Use ANALYZE to update statistics



DELIVERABLES:

\- schema.sql: Complete DDL statements

\- setup\_database.py: ETL script with error handling

\- Include data quality report (row counts, validation results)



CODE STYLE:

\- Use parameterized queries (prevent SQL injection)

\- Add docstrings

\- Include logging

\- Make it production-ready



BONUS: Add a function to generate a data quality report showing:

\- Row count per table

\- Foreign key validation results

\- Date range summaries

\- Any data anomalies found

