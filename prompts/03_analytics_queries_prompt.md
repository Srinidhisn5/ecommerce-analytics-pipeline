Create SQL analytics queries that provide BUSINESS INSIGHTS, not just data retrieval. Act as a business intelligence analyst.



OBJECTIVE: Write queries that answer strategic business questions.



REQUIRED QUERIES:



1\. CUSTOMER ANALYTICS:

&nbsp;  Query Name: high\_value\_customers

&nbsp;  Business Question: "Who are our top customers and what's their behavior?"

&nbsp;  

&nbsp;  Output columns:

&nbsp;  - customer\_name

&nbsp;  - total\_revenue (lifetime value)

&nbsp;  - order\_count

&nbsp;  - avg\_order\_value

&nbsp;  - first\_purchase\_date

&nbsp;  - last\_purchase\_date

&nbsp;  - customer\_tenure\_days

&nbsp;  - favorite\_category (most purchased)

&nbsp;  

&nbsp;  Filters: Top 20 customers

&nbsp;  Sorting: By total\_revenue DESC



2\. PRODUCT PERFORMANCE:

&nbsp;  Query Name: product\_performance\_by\_category

&nbsp;  Business Question: "Which products and categories drive our revenue?"

&nbsp;  

&nbsp;  JOIN: products, order\_items, reviews

&nbsp;  Output columns:

&nbsp;  - category

&nbsp;  - product\_count

&nbsp;  - total\_revenue

&nbsp;  - total\_units\_sold

&nbsp;  - avg\_unit\_price

&nbsp;  - avg\_rating

&nbsp;  - review\_count

&nbsp;  - revenue\_per\_product

&nbsp;  

&nbsp;  Include: Only products with sales

&nbsp;  Sorting: By total\_revenue DESC



3\. REVENUE TRENDS:

&nbsp;  Query Name: monthly\_revenue\_trends

&nbsp;  Business Question: "How is our business growing month-over-month?"

&nbsp;  

&nbsp;  Output columns:

&nbsp;  - year\_month (YYYY-MM format)

&nbsp;  - total\_revenue

&nbsp;  - order\_count

&nbsp;  - unique\_customers

&nbsp;  - avg\_order\_value

&nbsp;  - month\_over\_month\_growth (calculated)

&nbsp;  

&nbsp;  Time range: Last 12 months

&nbsp;  Include: Growth percentage calculation



4\. CUSTOMER COHORT ANALYSIS:

&nbsp;  Query Name: customer\_cohorts

&nbsp;  Business Question: "How do customers behave based on when they joined?"

&nbsp;  

&nbsp;  Group by: Registration month

&nbsp;  Output columns:

&nbsp;  - cohort\_month

&nbsp;  - customer\_count

&nbsp;  - total\_customers\_who\_purchased

&nbsp;  - purchase\_rate (%)

&nbsp;  - avg\_revenue\_per\_customer

&nbsp;  - avg\_orders\_per\_customer

&nbsp;  

&nbsp;  Insight: Show retention/engagement by cohort



5\. PRODUCT RATINGS ANALYSIS:

&nbsp;  Query Name: rating\_impact\_on\_sales

&nbsp;  Business Question: "Do higher-rated products sell better?"

&nbsp;  

&nbsp;  JOIN: products, reviews, order\_items

&nbsp;  Output columns:

&nbsp;  - rating\_bucket (1-2, 2-3, 3-4, 4-5)

&nbsp;  - product\_count

&nbsp;  - avg\_units\_sold

&nbsp;  - avg\_revenue\_per\_product

&nbsp;  - total\_revenue

&nbsp;  

&nbsp;  Insight: Correlation between ratings and sales



REQUIREMENTS:

\- Use CTEs for complex queries (improve readability)

\- Add comments explaining business logic

\- Use window functions where appropriate

\- Format numbers (2 decimal places for currency)

\- Handle NULL values gracefully

\- Optimize with proper JOINs (avoid cartesian products)



DELIVERABLES:

\- analytics.sql: All queries with documentation

\- run\_queries.py: Python script to execute queries and save results

\- insights.txt: Interpretation of results with business recommendations



Make the queries production-ready and performant!

