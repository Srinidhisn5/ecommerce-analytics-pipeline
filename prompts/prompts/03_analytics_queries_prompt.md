Create SQL analytics queries that provide BUSINESS INSIGHTS, not just data retrieval. Act as a business intelligence analyst.

OBJECTIVE: Write queries that answer strategic business questions.

REQUIRED QUERIES:

1. CUSTOMER ANALYTICS:
   Query Name: high_value_customers
   Business Question: "Who are our top customers and what's their behavior?"
   
   Output columns:
   - customer_name
   - total_revenue (lifetime value)
   - order_count
   - avg_order_value
   - first_purchase_date
   - last_purchase_date
   - customer_tenure_days
   - favorite_category (most purchased)
   
   Filters: Top 20 customers
   Sorting: By total_revenue DESC

2. PRODUCT PERFORMANCE:
   Query Name: product_performance_by_category
   Business Question: "Which products and categories drive our revenue?"
   
   JOIN: products, order_items, reviews
   Output columns:
   - category
   - product_count
   - total_revenue
   - total_units_sold
   - avg_unit_price
   - avg_rating
   - review_count
   - revenue_per_product
   
   Include: Only products with sales
   Sorting: By total_revenue DESC

3. REVENUE TRENDS:
   Query Name: monthly_revenue_trends
   Business Question: "How is our business growing month-over-month?"
   
   Output columns:
   - year_month (YYYY-MM format)
   - total_revenue
   - order_count
   - unique_customers
   - avg_order_value
   - month_over_month_growth (calculated)
   
   Time range: Last 12 months
   Include: Growth percentage calculation

4. CUSTOMER COHORT ANALYSIS:
   Query Name: customer_cohorts
   Business Question: "How do customers behave based on when they joined?"
   
   Group by: Registration month
   Output columns:
   - cohort_month
   - customer_count
   - total_customers_who_purchased
   - purchase_rate (%)
   - avg_revenue_per_customer
   - avg_orders_per_customer
   
   Insight: Show retention/engagement by cohort

5. PRODUCT RATINGS ANALYSIS:
   Query Name: rating_impact_on_sales
   Business Question: "Do higher-rated products sell better?"
   
   JOIN: products, reviews, order_items
   Output columns:
   - rating_bucket (1-2, 2-3, 3-4, 4-5)
   - product_count
   - avg_units_sold
   - avg_revenue_per_product
   - total_revenue
   
   Insight: Correlation between ratings and sales

REQUIREMENTS:
- Use CTEs for complex queries (improve readability)
- Add comments explaining business logic
- Use window functions where appropriate
- Format numbers (2 decimal places for currency)
- Handle NULL values gracefully
- Optimize with proper JOINs (avoid cartesian products)

DELIVERABLES:
- analytics.sql: All queries with documentation
- run_queries.py: Python script to execute queries and save results
- insights.txt: Interpretation of results with business recommendations

Make the queries production-ready and performant!