-- =============================================================================
-- E-COMMERCE ANALYTICS QUERIES
-- =============================================================================
-- These queries deliver strategic business insights using the SQLite database
-- located at database/ecommerce.db. Each query uses common table expressions
-- (CTEs) and window functions where appropriate to keep logic readable and
-- production-ready.
-- =============================================================================

-- Query: high_value_customers
-- Business Question: Who are our top customers and what's their behavior?
WITH completed_orders AS (
    SELECT *
    FROM orders
    WHERE status = 'Completed'
),
customer_order_summary AS (
    SELECT
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        SUM(co.total_amount) AS total_revenue,
        COUNT(DISTINCT co.order_id) AS order_count,
        AVG(co.total_amount) AS avg_order_value,
        MIN(co.order_date) AS first_purchase_date,
        MAX(co.order_date) AS last_purchase_date
    FROM customers c
    JOIN completed_orders co ON c.customer_id = co.customer_id
    GROUP BY c.customer_id
),
customer_category_revenue AS (
    SELECT
        co.customer_id,
        p.category,
        SUM(oi.line_total) AS category_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY co.customer_id
            ORDER BY SUM(oi.line_total) DESC, p.category
        ) AS category_rank
    FROM completed_orders co
    JOIN order_items oi ON co.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY co.customer_id, p.category
),
ranked_customers AS (
    SELECT
        cos.customer_id,
        cos.customer_name,
        ROUND(cos.total_revenue, 2) AS total_revenue,
        cos.order_count,
        ROUND(cos.avg_order_value, 2) AS avg_order_value,
        cos.first_purchase_date,
        cos.last_purchase_date,
        CAST(
            CASE
                WHEN cos.first_purchase_date IS NOT NULL
                     AND cos.last_purchase_date IS NOT NULL
                THEN JULIANDAY(cos.last_purchase_date) - JULIANDAY(cos.first_purchase_date)
                ELSE 0
            END AS INTEGER
        ) AS customer_tenure_days,
        ccr.category AS favorite_category,
        ROW_NUMBER() OVER (ORDER BY cos.total_revenue DESC) AS revenue_rank
    FROM customer_order_summary cos
    LEFT JOIN customer_category_revenue ccr
        ON cos.customer_id = ccr.customer_id AND ccr.category_rank = 1
)
SELECT
    customer_name,
    total_revenue,
    order_count,
    avg_order_value,
    first_purchase_date,
    last_purchase_date,
    customer_tenure_days,
    COALESCE(favorite_category, 'N/A') AS favorite_category
FROM ranked_customers
WHERE revenue_rank <= 20
ORDER BY total_revenue DESC;


-- Query: product_performance_by_category
-- Business Question: Which products and categories drive our revenue?
WITH completed_orders AS (
    SELECT *
    FROM orders
    WHERE status = 'Completed'
),
product_sales AS (
    SELECT
        p.product_id,
        p.category,
        SUM(oi.line_total) AS total_revenue,
        SUM(oi.quantity) AS total_units_sold,
        SUM(oi.unit_price * oi.quantity) AS weighted_unit_price_sum
    FROM order_items oi
    JOIN completed_orders co ON oi.order_id = co.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY p.product_id, p.category
),
product_reviews AS (
    SELECT
        r.product_id,
        AVG(r.rating) AS avg_rating,
        COUNT(r.review_id) AS review_count
    FROM reviews r
    GROUP BY r.product_id
),
category_performance AS (
    SELECT
        ps.category,
        COUNT(DISTINCT ps.product_id) AS product_count,
        SUM(ps.total_revenue) AS total_revenue,
        SUM(ps.total_units_sold) AS total_units_sold,
        SUM(ps.weighted_unit_price_sum) AS weighted_unit_price_sum,
        SUM(COALESCE(pr.review_count, 0)) AS review_count,
        SUM(COALESCE(pr.avg_rating * pr.review_count, 0)) AS weighted_rating_sum
    FROM product_sales ps
    LEFT JOIN product_reviews pr ON ps.product_id = pr.product_id
    GROUP BY ps.category
)
SELECT
    category,
    product_count,
    ROUND(total_revenue, 2) AS total_revenue,
    total_units_sold,
    ROUND(
        CASE WHEN total_units_sold > 0
             THEN weighted_unit_price_sum / total_units_sold
             ELSE 0 END, 2
    ) AS avg_unit_price,
    ROUND(
        CASE WHEN review_count > 0
             THEN weighted_rating_sum / review_count
             ELSE NULL END, 2
    ) AS avg_rating,
    review_count,
    ROUND(
        CASE WHEN product_count > 0
             THEN total_revenue * 1.0 / product_count
             ELSE 0 END, 2
    ) AS revenue_per_product
FROM category_performance
ORDER BY total_revenue DESC;


-- Query: monthly_revenue_trends
-- Business Question: How is our business growing month-over-month?
WITH completed_orders AS (
    SELECT *
    FROM orders
    WHERE status = 'Completed'
),
period_bounds AS (
    SELECT
        DATE(MAX(order_date), 'start of month') AS max_month_start,
        DATE(MAX(order_date), 'start of month', '-11 months') AS start_month
    FROM completed_orders
),
monthly_base AS (
    SELECT
        strftime('%Y-%m', co.order_date) AS year_month,
        SUM(co.total_amount) AS total_revenue,
        COUNT(DISTINCT co.order_id) AS order_count,
        COUNT(DISTINCT co.customer_id) AS unique_customers
    FROM completed_orders co
    WHERE co.order_date >= (SELECT start_month FROM period_bounds)
    GROUP BY strftime('%Y-%m', co.order_date)
),
monthly_enriched AS (
    SELECT
        year_month,
        ROUND(total_revenue, 2) AS total_revenue,
        order_count,
        unique_customers,
        ROUND(
            CASE WHEN order_count > 0
                 THEN total_revenue / order_count
                 ELSE 0 END, 2
        ) AS avg_order_value,
        LAG(total_revenue) OVER (ORDER BY year_month) AS prev_revenue
    FROM monthly_base
)
SELECT
    year_month,
    total_revenue,
    order_count,
    unique_customers,
    avg_order_value,
    ROUND(
        CASE WHEN prev_revenue IS NULL OR prev_revenue = 0
             THEN NULL
             ELSE (total_revenue - prev_revenue) * 100.0 / prev_revenue
        END, 2
    ) AS month_over_month_growth
FROM monthly_enriched
ORDER BY year_month;


-- Query: customer_cohorts
-- Business Question: How do customers behave based on when they joined?
WITH cohort_customers AS (
    SELECT
        customer_id,
        strftime('%Y-%m', registration_date) AS cohort_month
    FROM customers
),
customer_activity AS (
    SELECT
        co.customer_id,
        COUNT(DISTINCT co.order_id) AS order_count,
        SUM(co.total_amount) AS revenue
    FROM orders co
    WHERE co.status = 'Completed'
    GROUP BY co.customer_id
),
cohort_aggregation AS (
    SELECT
        cc.cohort_month,
        COUNT(DISTINCT cc.customer_id) AS customer_count,
        COUNT(DISTINCT CASE WHEN ca.order_count > 0 THEN cc.customer_id END) AS purchasing_customers,
        SUM(COALESCE(ca.revenue, 0)) AS total_revenue,
        SUM(COALESCE(ca.order_count, 0)) AS total_orders
    FROM cohort_customers cc
    LEFT JOIN customer_activity ca ON cc.customer_id = ca.customer_id
    GROUP BY cc.cohort_month
)
SELECT
    cohort_month,
    customer_count,
    purchasing_customers AS total_customers_who_purchased,
    ROUND(
        CASE WHEN customer_count > 0
             THEN purchasing_customers * 100.0 / customer_count
             ELSE 0 END, 2
    ) AS purchase_rate,
    ROUND(
        CASE WHEN customer_count > 0
             THEN total_revenue * 1.0 / customer_count
             ELSE 0 END, 2
    ) AS avg_revenue_per_customer,
    ROUND(
        CASE WHEN customer_count > 0
             THEN total_orders * 1.0 / customer_count
             ELSE 0 END, 2
    ) AS avg_orders_per_customer
FROM cohort_aggregation
ORDER BY cohort_month;


-- Query: rating_impact_on_sales
-- Business Question: Do higher-rated products sell better?
WITH completed_orders AS (
    SELECT *
    FROM orders
    WHERE status = 'Completed'
),
product_sales AS (
    SELECT
        p.product_id,
        SUM(oi.quantity) AS total_units_sold,
        SUM(oi.line_total) AS total_revenue
    FROM order_items oi
    JOIN completed_orders co ON oi.order_id = co.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY p.product_id
),
product_ratings AS (
    SELECT
        r.product_id,
        AVG(r.rating) AS avg_rating
    FROM reviews r
    GROUP BY r.product_id
),
product_metrics AS (
    SELECT
        ps.product_id,
        pr.avg_rating,
        ps.total_units_sold,
        ps.total_revenue,
        CASE
            WHEN pr.avg_rating IS NULL THEN NULL
            WHEN pr.avg_rating < 2 THEN '1-2'
            WHEN pr.avg_rating < 3 THEN '2-3'
            WHEN pr.avg_rating < 4 THEN '3-4'
            ELSE '4-5'
        END AS rating_bucket
    FROM product_sales ps
    LEFT JOIN product_ratings pr ON ps.product_id = pr.product_id
)
SELECT
    rating_bucket,
    COUNT(*) AS product_count,
    ROUND(
        CASE WHEN COUNT(*) > 0
             THEN SUM(total_units_sold) * 1.0 / COUNT(*)
             ELSE 0 END, 2
    ) AS avg_units_sold,
    ROUND(
        CASE WHEN COUNT(*) > 0
             THEN SUM(total_revenue) * 1.0 / COUNT(*)
             ELSE 0 END, 2
    ) AS avg_revenue_per_product,
    ROUND(SUM(total_revenue), 2) AS total_revenue
FROM product_metrics
WHERE rating_bucket IS NOT NULL
GROUP BY rating_bucket
ORDER BY rating_bucket;

