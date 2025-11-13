# 🛍️ E-Commerce Analytics Pipeline
### AI-Assisted Development with Cursor IDE

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![SQLite](https://img.shields.io/badge/SQLite-3-green.svg)
![Cursor](https://img.shields.io/badge/Built%20with-Cursor%20IDE-purple.svg)

## 📋 Executive Summary

This project demonstrates the power of AI-assisted development by building a complete e-commerce data analytics pipeline in 30 minutes. Using Cursor IDE's AI capabilities, I created a production-ready data system that would typically take hours to build manually.

**Key Achievement:** Transformed business requirements into working code through strategic AI prompting, showcasing the future of software development.

## 🎯 Project Objectives

1. ✅ Generate realistic synthetic e-commerce data (5 interconnected datasets)
2. ✅ Design and implement normalized database schema
3. ✅ Create complex analytical queries with multi-table joins
4. ✅ Deliver actionable business insights

## 🏗️ Architecture

```
Data Generation → Data Validation → Database Design → ETL Pipeline → Analytics Layer
```

### Data Model
- **Products**: Inventory management with categories and pricing
- **Customers**: User profiles with demographic data
- **Orders**: Transaction records with timestamps
- **Order Items**: Line-level detail linking orders to products
- **Reviews**: Customer feedback with ratings and sentiment

## 🔑 Key Features

### 1. **Intelligent Data Generation**
- Realistic relationships between entities
- Date consistency (orders after customer creation)
- Price calculations with taxes and discounts
- Review ratings correlated with product quality

### 2. **Robust Database Design**
- Normalized schema (3NF)
- Foreign key constraints
- Proper indexing for query performance
- Data type optimization

### 3. **Advanced Analytics**
- Customer Lifetime Value (CLV) analysis
- Cohort analysis by registration month
- Product performance metrics
- Revenue trend analysis
- Customer segmentation

## 📊 Business Insights Generated

Our queries answer critical business questions:

1. **Who are our most valuable customers?**
   - Top customers by revenue contribution
   - Purchase frequency and average order value

2. **Which products drive our business?**
   - Best sellers by category
   - Profit margins by product line
   - Inventory turnover rates

3. **How is our business trending?**
   - Month-over-month revenue growth
   - Customer acquisition trends
   - Seasonal patterns

4. **What do customers think?**
   - Product ratings by category
   - Review sentiment analysis
   - Correlation between ratings and sales

## 🚀 Quick Start

```bash
# Clone repository
git clone <your-repo-url>
cd ecommerce-analytics-pipeline

# Install dependencies
pip install -r requirements.txt

# Generate synthetic data
python scripts/generate_data.py

# Setup database and load data
python scripts/setup_database.py

# Run analytics queries
python scripts/run_queries.py
```

## 📁 Project Structure

```
ecommerce-analytics-pipeline/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── prompts/                           # AI prompts used
│   ├── 01_data_generation.md
│   ├── 02_database_design.md
│   └── 03_analytics_queries.md
├── data/                              # Generated datasets
│   ├── products.csv
│   ├── customers.csv
│   ├── orders.csv
│   ├── order_items.csv
│   └── reviews.csv
├── scripts/                           # Python automation
│   ├── generate_data.py
│   ├── setup_database.py
│   ├── run_queries.py
│   └── utils.py
├── database/                          # SQLite database
│   └── ecommerce.db
├── queries/                           # SQL queries
│   ├── schema.sql
│   └── analytics.sql
├── results/                           # Query outputs
│   └── insights.txt
└── docs/                              # Additional documentation
    └── data_dictionary.md
```

## 💡 AI Prompting Strategy

### My Approach to Working with Cursor

Instead of generating code randomly, I used a **structured prompting methodology**:

1. **Context Setting**: Provided clear business requirements
2. **Constraint Definition**: Specified technical limitations and standards
3. **Iterative Refinement**: Improved outputs through follow-up prompts
4. **Validation Requests**: Asked AI to verify data integrity

**Example Prompt Pattern:**
```
"As an experienced data engineer, create a [specific component]
that [specific requirements]. Ensure [quality constraints].
Consider [edge cases]. Follow [standards]."
```

### Key Learnings

✅ **Specific prompts > Vague requests**
✅ **Provide context and constraints**
✅ **Review and validate AI output**
✅ **Iterate for quality improvements**
✅ **Document everything**

## 📈 Sample Outputs

### Top 5 Customers by Revenue
| Customer | Total Spent | Orders | Avg Order |
|----------|-------------|--------|-----------|
| Alice Johnson | $45,230 | 23 | $1,966 |
| Bob Smith | $38,450 | 19 | $2,024 |
| ... | ... | ... | ... |

### Product Performance
| Category | Products | Total Revenue | Avg Rating |
|----------|----------|---------------|------------|
| Electronics | 25 | $234,500 | 4.3 |
| Clothing | 40 | $187,300 | 4.5 |
| ... | ... | ... | ... |

## 🛠️ Technologies Used

- **Python 3.8+**: Data generation and ETL
- **SQLite3**: Database management
- **Pandas**: Data manipulation
- **Faker**: Synthetic data generation
- **Cursor IDE**: AI-assisted development
- **Git/GitHub**: Version control

## 🎓 Key Takeaways

### Technical Skills Demonstrated
- Database design and normalization
- ETL pipeline development
- SQL query optimization
- Python scripting
- Data validation

### AI-Assisted Development Skills
- Effective prompt engineering
- Code review and validation
- Iterative refinement
- Documentation generation

### Business Acumen
- Understanding of e-commerce metrics
- Analytical thinking
- Data-driven insights
- Stakeholder communication

## 🔮 Future Enhancements

If given more time, I would add:
- [ ] Data visualization dashboard
- [ ] Automated data quality checks
- [ ] API layer for data access
- [ ] Real-time streaming data pipeline
- [ ] Machine learning models for predictions

## 👤 About This Project

This project was completed as part of the Cursor IDE development challenge. It demonstrates proficiency in:
- Modern AI-assisted development workflows
- Database design and SQL
- Python programming
- Business analytics
- Professional documentation

**Time to Complete**: 30 minutes (as required)
**Lines of Code Generated**: ~500
**Commits**: 8 (showing iterative development)

## 📝 License

MIT License - feel free to use this as a learning resource

## 🙏 Acknowledgments

Built with assistance from Cursor IDE - demonstrating the future of software development where humans and AI collaborate to build better software, faster.

---

**Note**: All data in this project is synthetically generated and does not represent any real business or individuals.