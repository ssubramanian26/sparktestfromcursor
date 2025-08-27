#!/usr/bin/env python3
"""
Quick Test Script for PySpark Orders Processing Application
Creates smaller sample data for testing purposes before running the full application.
"""

import pandas as pd
import numpy as np
from faker import Faker
import os
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import random
import sys


def create_small_test_data():
    """Create small test parquet files for quick validation"""
    
    fake = Faker()
    Faker.seed(42)
    np.random.seed(42)
    random.seed(42)
    
    print("Creating small test dataset for validation...")
    
    # Product categories and names for more realistic data
    PRODUCT_CATEGORIES = [
        'Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books'
    ]
    
    PRODUCTS = {
        'Electronics': ['Smartphone', 'Laptop', 'Tablet'],
        'Clothing': ['T-Shirt', 'Jeans', 'Dress'],
        'Home & Garden': ['Furniture', 'Kitchen Appliances'],
        'Sports': ['Running Shoes', 'Fitness Equipment'],
        'Books': ['Fiction Novel', 'Non-Fiction']
    }
    
    # Create test data directory
    test_dir = 'test_data'
    os.makedirs(test_dir, exist_ok=True)
    
    # Create 3 small test files with 1000 rows each
    for file_num in range(1, 4):
        print(f"Creating test file {file_num}/3...")
        
        data = []
        for _ in range(1000):
            # Basic order info
            order_id = fake.uuid4()
            customer_id = fake.uuid4()
            
            # Customer info
            customer_name = fake.name()
            customer_email = fake.email()
            customer_phone = fake.phone_number()
            
            # Order timing
            order_date = fake.date_between(start_date='-1y', end_date='today')
            
            # Product info
            category = random.choice(PRODUCT_CATEGORIES)
            product_name = random.choice(PRODUCTS[category])
            product_id = fake.uuid4()
            
            # Order details
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(10.0, 200.0), 2)
            total_amount = round(quantity * unit_price, 2)
            
            # Shipping info
            shipping_address = fake.address().replace('\n', ', ')
            shipping_city = fake.city()
            shipping_state = fake.state()
            shipping_zip = fake.zipcode()
            shipping_country = fake.country()
            
            # Additional fields
            order_status = random.choice(['Pending', 'Processing', 'Shipped', 'Delivered'])
            payment_method = random.choice(['Credit Card', 'Debit Card', 'PayPal'])
            discount_amount = round(random.uniform(0, total_amount * 0.1), 2)
            tax_amount = round(total_amount * 0.08, 2)
            final_amount = round(total_amount - discount_amount + tax_amount, 2)
            
            # Store additional metadata
            store_id = random.randint(1000, 1999)
            sales_rep_id = random.randint(100, 199)
            promotion_code = fake.lexify(text='TEST????') if random.random() > 0.8 else None
            
            data.append({
                'order_id': order_id,
                'customer_id': customer_id,
                'customer_name': customer_name,
                'customer_email': customer_email,
                'customer_phone': customer_phone,
                'order_date': order_date,
                'product_id': product_id,
                'product_name': product_name,
                'product_category': category,
                'quantity': quantity,
                'unit_price': unit_price,
                'total_amount': total_amount,
                'discount_amount': discount_amount,
                'tax_amount': tax_amount,
                'final_amount': final_amount,
                'shipping_address': shipping_address,
                'shipping_city': shipping_city,
                'shipping_state': shipping_state,
                'shipping_zip': shipping_zip,
                'shipping_country': shipping_country,
                'order_status': order_status,
                'payment_method': payment_method,
                'store_id': store_id,
                'sales_rep_id': sales_rep_id,
                'promotion_code': promotion_code
            })
        
        # Create DataFrame and save as parquet
        df = pd.DataFrame(data)
        filename = os.path.join(test_dir, f'test_orders_part_{file_num:02d}.parquet')
        df.to_parquet(filename, index=False)
        
        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        print(f"  - Created {filename}: {file_size_mb:.2f} MB, {len(df)} rows")
    
    print(f"\nTest data created in: {os.path.abspath(test_dir)}")
    return test_dir


def run_quick_test():
    """Run a quick test of the PySpark application with small data"""
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import count, desc
        
        print("\nInitializing Spark for quick test...")
        
        spark = SparkSession.builder \
            .appName("QuickTest") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        # Load test data
        test_dir = 'test_data'
        df = spark.read.parquet(f"{test_dir}/*.parquet")
        
        # Basic validation
        total_rows = df.count()
        print(f"✅ Loaded {total_rows} rows from test data")
        
        # Show schema
        print("\nSchema validation:")
        df.printSchema()
        
        # Show first 10 rows
        print("\nFirst 10 rows:")
        df.show(10, truncate=False)
        
        # Quick analysis
        print("\nQuick analysis:")
        print("Orders by status:")
        df.groupBy("order_status").count().show()
        
        print("Product categories:")
        df.groupBy("product_category").count().show()
        
        # Test export
        output_dir = 'test_output'
        os.makedirs(output_dir, exist_ok=True)
        
        sample_df = df.limit(10)
        sample_df.coalesce(1).write.mode("overwrite").parquet(f"{output_dir}/sample_10_rows")
        
        print(f"✅ Sample data exported to: {os.path.abspath(output_dir)}")
        
        spark.stop()
        print("✅ Quick test completed successfully!")
        
        return True
        
    except Exception as e:
        print(f"❌ Quick test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def cleanup_test_data():
    """Clean up test data files"""
    import shutil
    
    test_dirs = ['test_data', 'test_output']
    for directory in test_dirs:
        if os.path.exists(directory):
            shutil.rmtree(directory)
            print(f"Cleaned up: {directory}")


def main():
    """Main function for quick test"""
    
    print("=" * 60)
    print("PYSPARK ORDERS PROCESSING - QUICK TEST")
    print("=" * 60)
    
    try:
        # Create small test data
        test_dir = create_small_test_data()
        
        # Run quick test
        success = run_quick_test()
        
        if success:
            print("\n" + "=" * 60)
            print("✅ QUICK TEST PASSED!")
            print("\nYour system is ready for the full application.")
            print("You can now run:")
            print("1. python generate_sample_data.py  # For full 100GB dataset")
            print("2. python spark_orders_processor.py  # Process the data")
        else:
            print("\n" + "=" * 60)
            print("❌ QUICK TEST FAILED!")
            print("Please check the error messages above.")
        
        # Ask user if they want to clean up test data
        if input("\nClean up test data files? (y/N): ").lower().startswith('y'):
            cleanup_test_data()
            print("Test data cleaned up.")
        
        print("=" * 60)
        
        return success
        
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
