-- ============================================================================
-- E-Commerce Analytics Database Schema
-- ============================================================================
-- Database: SQLite
-- Design: Normalized 3NF schema with referential integrity
-- Purpose: Analytics-ready data warehouse for e-commerce operations
-- ============================================================================

-- Enable foreign key constraints
PRAGMA foreign_keys = ON;

-- ============================================================================
-- PRODUCTS TABLE
-- ============================================================================
-- Stores product catalog information
-- Normalization: 3NF (no transitive dependencies)
-- ============================================================================

CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    subcategory TEXT NOT NULL,
    price REAL NOT NULL CHECK (price > 0),
    cost REAL NOT NULL CHECK (cost >= 0 AND cost < price),
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0),
    supplier TEXT NOT NULL,
    created_date DATE NOT NULL,
    -- Business rule: profit margin should be between 20-50%
    CHECK ((price - cost) / price >= 0.20 AND (price - cost) / price <= 0.50)
);

-- Index on category for filtering and analytics
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);

-- Index on subcategory for drill-down queries
CREATE INDEX IF NOT EXISTS idx_products_subcategory ON products(subcategory);

-- Index on created_date for temporal analysis
CREATE INDEX IF NOT EXISTS idx_products_created_date ON products(created_date);

-- ============================================================================
-- CUSTOMERS TABLE
-- ============================================================================
-- Stores customer master data
-- Normalization: 3NF (address components normalized)
-- ============================================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    phone TEXT,
    address TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    zip TEXT NOT NULL,
    country TEXT NOT NULL DEFAULT 'USA',
    registration_date DATE NOT NULL,
    -- Email format validation (basic)
    CHECK (email LIKE '%@%.%')
);

-- Index on email for lookups
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);

-- Index on registration_date for cohort analysis
CREATE INDEX IF NOT EXISTS idx_customers_registration_date ON customers(registration_date);

-- Index on state for geographic analysis
CREATE INDEX IF NOT EXISTS idx_customers_state ON customers(state);

-- Composite index for name searches
CREATE INDEX IF NOT EXISTS idx_customers_name ON customers(last_name, first_name);

-- ============================================================================
-- ORDERS TABLE
-- ============================================================================
-- Stores order header information
-- Normalization: 3NF (shipping address denormalized for performance)
-- ============================================================================

CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('Completed', 'Processing', 'Cancelled', 'Returned')),
    payment_method TEXT NOT NULL CHECK (payment_method IN ('Credit Card', 'PayPal', 'Debit')),
    shipping_address TEXT NOT NULL,
    shipping_city TEXT NOT NULL,
    shipping_state TEXT NOT NULL,
    shipping_zip TEXT NOT NULL,
    shipping_country TEXT NOT NULL DEFAULT 'USA',
    total_amount REAL NOT NULL CHECK (total_amount >= 0),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE RESTRICT,
    -- Business rule: order date must be after customer registration
    -- This will be validated in application logic as SQLite doesn't support
    -- cross-table CHECK constraints
    CHECK (order_date >= '2023-01-01')
);

-- Index on customer_id for customer order history queries
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);

-- Index on order_date for temporal analysis
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);

-- Index on status for filtering by order status
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

-- Composite index for customer order date queries
CREATE INDEX IF NOT EXISTS idx_orders_customer_date ON orders(customer_id, order_date);

-- ============================================================================
-- ORDER_ITEMS TABLE
-- ============================================================================
-- Stores order line items (many-to-many relationship between orders and products)
-- Normalization: 3NF (line_total is calculated, stored for performance)
-- ============================================================================

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price REAL NOT NULL CHECK (unit_price > 0),
    discount REAL NOT NULL CHECK (discount >= 0 AND discount <= 1),
    line_total REAL NOT NULL CHECK (line_total >= 0),
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE RESTRICT,
    -- Business rule: line_total should equal quantity * unit_price * (1 - discount)
    -- Using approximate check due to floating point precision
    CHECK (ABS(line_total - (quantity * unit_price * (1 - discount))) < 0.01)
);

-- Index on order_id for order detail queries
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);

-- Index on product_id for product sales analysis
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);

-- Composite index for order-product lookups
CREATE INDEX IF NOT EXISTS idx_order_items_order_product ON order_items(order_id, product_id);

-- ============================================================================
-- REVIEWS TABLE
-- ============================================================================
-- Stores product reviews by customers
-- Normalization: 3NF (review_text is atomic)
-- ============================================================================

CREATE TABLE IF NOT EXISTS reviews (
    review_id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    review_text TEXT,
    review_date DATE NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE,
    -- Business rule: review date should be after order date
    -- Validated in application logic
    CHECK (review_date >= '2023-01-01')
);

-- Index on product_id for product review aggregation
CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews(product_id);

-- Index on customer_id for customer review history
CREATE INDEX IF NOT EXISTS idx_reviews_customer_id ON reviews(customer_id);

-- Index on rating for rating distribution analysis
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);

-- Composite index for product rating queries
CREATE INDEX IF NOT EXISTS idx_reviews_product_rating ON reviews(product_id, rating);

-- Index on review_date for temporal analysis
CREATE INDEX IF NOT EXISTS idx_reviews_review_date ON reviews(review_date);

-- ============================================================================
-- VIEWS FOR ANALYTICS
-- ============================================================================

-- View: Product Sales Summary
CREATE VIEW IF NOT EXISTS v_product_sales_summary AS
SELECT 
    p.product_id,
    p.name,
    p.category,
    p.subcategory,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.line_total) AS total_revenue,
    AVG(oi.unit_price) AS avg_unit_price,
    AVG(oi.discount) AS avg_discount
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.order_id AND o.status = 'Completed'
GROUP BY p.product_id, p.name, p.category, p.subcategory;

-- View: Customer Order Summary
CREATE VIEW IF NOT EXISTS v_customer_order_summary AS
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    c.registration_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(CASE WHEN o.status = 'Completed' THEN o.total_amount ELSE 0 END) AS total_spent,
    AVG(CASE WHEN o.status = 'Completed' THEN o.total_amount ELSE NULL END) AS avg_order_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.registration_date;

-- View: Monthly Sales Summary
CREATE VIEW IF NOT EXISTS v_monthly_sales_summary AS
SELECT 
    strftime('%Y-%m', o.order_date) AS month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    SUM(oi.line_total) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.status = 'Completed'
GROUP BY strftime('%Y-%m', o.order_date)
ORDER BY month;

-- ============================================================================
-- ANALYZE STATISTICS
-- ============================================================================
-- Update query planner statistics for optimal query performance
-- ============================================================================

ANALYZE;

