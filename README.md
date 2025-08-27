# PySpark Orders Processing Application

A high-performance PySpark application that processes large-scale customer order data from multiple parquet files. This application demonstrates big data processing capabilities by handling 10 files of approximately 10GB each, containing millions of rows of customer order information.

## Features

- **Large-scale data processing**: Handles 100GB+ of customer order data
- **Distributed processing**: Utilizes Apache Spark for efficient parallel processing
- **Interactive web interface**: Streamlit-based dashboard for data exploration and filtering
- **Data generation**: Includes a comprehensive data generator for creating realistic sample data
- **Analytics capabilities**: Performs various analyses on the order data
- **Optimized performance**: Configured with Spark optimizations for large dataset processing
- **Sample extraction**: Outputs the first 100 rows for quick data inspection

## Project Structure

```
Spark Application/
├── generate_sample_data.py    # Script to generate sample parquet files
├── spark_orders_processor.py  # Main PySpark processing application
├── streamlit_app.py           # Interactive web dashboard for data exploration
├── run_streamlit.py           # Streamlit launcher script
├── setup.py                  # System setup validation script
├── quick_test.py             # Quick test with small sample data
├── requirements.txt          # Python dependencies
├── README.md                 # This documentation
├── data/                     # Directory for generated parquet files (created by generator)
├── test_data/                # Directory for test parquet files (created by quick_test)
└── output/                   # Directory for processed results (created by processor)
```

## Prerequisites

### System Requirements
- **Python**: 3.8 or higher
- **Java**: JDK 8 or 11 (required for Spark)
- **Memory**: Minimum 16GB RAM recommended (32GB+ for optimal performance)
- **Disk Space**: ~150GB free space (100GB for data + overhead)
- **CPU**: Multi-core processor recommended

### Java Installation

#### macOS (using Homebrew):
```bash
brew install openjdk@11
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
```

#### Linux (Ubuntu/Debian):
```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

#### Set JAVA_HOME:
```bash
# Add to your ~/.bashrc or ~/.zshrc
export JAVA_HOME=$(/usr/libexec/java_home -v 11)  # macOS
# or
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # Linux
```

## Installation

1. **Clone or create the project directory**:
   ```bash
   mkdir -p "Spark Application"
   cd "Spark Application"
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv spark_env
   source spark_env/bin/activate  # On Windows: spark_env\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Verify Spark installation**:
   ```bash
   python -c "from pyspark.sql import SparkSession; print('PySpark installed successfully!')"
   ```

## Usage

### Step 1: Generate Sample Data

First, generate the sample parquet files containing customer order data:

```bash
python generate_sample_data.py
```

**This process will:**
- Create 10 parquet files (~10GB each)
- Generate millions of rows of realistic customer order data
- Store files in the `data/` directory
- Take approximately 30-60 minutes depending on your system

**Sample data includes:**
- Order information (ID, date, status)
- Customer details (name, email, phone, shipping address)
- Product information (ID, name, category)
- Financial data (prices, discounts, taxes, final amounts)
- Store and sales representative information

### Step 2: Process the Data

Once the data is generated, run the main processing application:

```bash
python spark_orders_processor.py
```

**This will:**
- Load all 10 parquet files into a single Spark DataFrame
- Display the first 100 rows
- Perform various analytics on the data
- Export sample results to the `output/` directory

### Step 3: Interactive Web Dashboard (Streamlit)

For a more interactive experience, launch the Streamlit web application:

```bash
python3 run_streamlit.py
```

**The Streamlit dashboard provides:**
- **Interactive data filtering**: Filter by date range, product category, order status, payment method, and revenue range
- **Row count control**: Dynamically adjust the number of rows to display (10 to 10,000)
- **Real-time visualizations**: Charts and graphs that update based on your filters
- **Data exploration tabs**:
  - 📊 Sales Analysis (order status, payment methods, revenue distribution)
  - 🛍️ Product Analysis (category performance, top products)
  - 📅 Time Analysis (daily orders and revenue trends)
  - 🌍 Geographic Analysis (orders by state and region)
- **Interactive data table**: View and download filtered data as CSV
- **Column selection**: Choose which columns to display in the table

The app will automatically open in your browser at `http://localhost:8501`

## Sample Output

The application will provide:

1. **Data Overview**: Schema and basic statistics
2. **First 100 Rows**: Complete display of the first 100 records
3. **Analytics**:
   - Orders by status distribution
   - Top product categories by revenue
   - Monthly order trends
   - Payment method analysis
   - Geographic distribution by state
4. **Exported Data**: First 100 rows saved as parquet file in `output/`

## Performance Tuning

The application includes several Spark optimizations:

- **Adaptive Query Execution**: Automatically optimizes joins and aggregations
- **Arrow Integration**: Faster data transfer between Python and JVM
- **Partition Coalescing**: Reduces small file overhead
- **Caching**: Keeps frequently accessed data in memory
- **Column Pruning**: Only reads necessary columns
- **Predicate Pushdown**: Filters data at the source level

### Memory Configuration

For different system configurations:

**16GB RAM System:**
```python
.config("spark.driver.memory", "4g") \
.config("spark.executor.memory", "4g")
```

**32GB+ RAM System:**
```python
.config("spark.driver.memory", "8g") \
.config("spark.executor.memory", "8g")
```

## Troubleshooting

### Common Issues

1. **Java Not Found**:
   ```
   Error: JAVA_HOME is not set
   ```
   **Solution**: Install Java and set JAVA_HOME environment variable

2. **OutOfMemory Errors**:
   ```
   Java heap space OutOfMemoryError
   ```
   **Solution**: Increase driver/executor memory in the Spark configuration

3. **Disk Space Issues**:
   ```
   No space left on device
   ```
   **Solution**: Ensure you have 150GB+ free disk space

4. **Permission Errors**:
   ```
   Permission denied
   ```
   **Solution**: Ensure write permissions for data/ and output/ directories

### Performance Tips

1. **SSD Storage**: Use SSD for data directory for faster I/O
2. **More RAM**: Increase system RAM for better caching
3. **Parallelism**: Adjust `spark.default.parallelism` based on CPU cores
4. **Batch Size**: Modify batch sizes in data generation for memory optimization

## Data Schema

The generated customer order data includes the following fields:

| Field | Type | Description |
|-------|------|-------------|
| order_id | string | Unique order identifier |
| customer_id | string | Unique customer identifier |
| customer_name | string | Customer full name |
| customer_email | string | Customer email address |
| customer_phone | string | Customer phone number |
| order_date | date | Date when order was placed |
| product_id | string | Unique product identifier |
| product_name | string | Product name |
| product_category | string | Product category |
| quantity | integer | Quantity ordered |
| unit_price | decimal | Price per unit |
| total_amount | decimal | Total before discounts and taxes |
| discount_amount | decimal | Discount applied |
| tax_amount | decimal | Tax amount |
| final_amount | decimal | Final amount after discounts and taxes |
| shipping_address | string | Complete shipping address |
| shipping_city | string | Shipping city |
| shipping_state | string | Shipping state |
| shipping_zip | string | Shipping ZIP code |
| shipping_country | string | Shipping country |
| order_status | string | Current order status |
| payment_method | string | Payment method used |
| store_id | integer | Store identifier |
| sales_rep_id | integer | Sales representative ID |
| promotion_code | string | Promotional code (if applicable) |

## Advanced Usage

### Custom Data Generation

You can modify `generate_sample_data.py` to:
- Change file sizes by adjusting `target_size_gb`
- Modify data schema by updating the `generate_batch_data()` function
- Add new product categories or customer attributes

### Custom Analytics

Extend `spark_orders_processor.py` to add:
- Customer lifetime value analysis
- Seasonal trend analysis
- Geographic sales patterns
- Product recommendation systems

### Integration

The processed data can be:
- Exported to databases (PostgreSQL, MySQL, etc.)
- Saved to data lakes (S3, HDFS, etc.)
- Connected to BI tools (Tableau, Power BI, etc.)
- Used for machine learning pipelines

## License

This project is provided as-is for educational and demonstration purposes.

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review Spark logs for detailed error messages
3. Ensure all prerequisites are properly installed
4. Verify system resources meet minimum requirements
