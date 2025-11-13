# Prompt 1: Data Generation

## Initial Prompt
[I need to create a realistic e-commerce dataset for analytics. Act as a senior data engineer.

REQUIREMENTS:
- Generate 5 interconnected CSV files
- Ensure referential integrity between files
- Use realistic business rules

FILES TO GENERATE:

1. products.csv (200 products)
   - product_id, name, category, subcategory, price, cost, stock_quantity, supplier, created_date
   - Categories: Electronics, Clothing, Home & Garden, Sports, Books
   - Profit margins should vary realistically (20-50%)

2. customers.csv (1000 customers)
   - customer_id, first_name, last_name, email, phone, address, city, state, zip, country, registration_date
   - Registration dates between Jan 2023 - Oct 2024
   - Realistic email patterns (firstname.lastname@domain.com)

3. orders.csv (3000 orders)
   - order_id, customer_id, order_date, status, payment_method, shipping_address, total_amount
   - Order dates MUST be after customer registration_date
   - Status: Completed (80%), Processing (10%), Cancelled (5%), Returned (5%)
   - Payment methods: Credit Card (60%), PayPal (25%), Debit (15%)

4. order_items.csv (8000 line items)
   - order_item_id, order_id, product_id, quantity, unit_price, discount
   - Average 2-3 items per order
   - Discounts: 0% (70%), 10% (20%), 20% (8%), 25% (2%)
   - Calculate line_total = quantity * unit_price * (1 - discount)

5. reviews.csv (2500 reviews)
   - review_id, product_id, customer_id, rating, review_text, review_date
   - Only for customers who bought that product
   - Ratings: 5 stars (40%), 4 stars (30%), 3 stars (15%), 2 stars (10%), 1 star (5%)
   - Review dates after purchase date

CRITICAL BUSINESS RULES:
- Higher-priced products get better reviews (simulate quality correlation)
- Some customers are "whales" (20% of customers generate 60% of revenue)
- Seasonal trends: higher sales in Nov-Dec
- Popular products have more reviews
- Electronics have higher average order values

CODE REQUIREMENTS:
- Use Faker library for realistic names/addresses
- Add data validation checks
- Include progress indicators
- Handle edge cases
- Use pandas for efficient CSV generation
- Add comments explaining business logic

OUTPUT: Clean, documented Python script that generates production-quality synthetic data.]

## Follow-up Prompts
(If you refined anything)

## Notes
Cursor generated the data generator script from this prompt.
