#!/usr/bin/env python3
"""
Data Generator Script for PySpark Application
Generates 10 parquet files, each approximately 10GB with millions of rows of customer order data.
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
from tqdm import tqdm

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Product categories and names for more realistic data
PRODUCT_CATEGORIES = [
    'Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 
    'Beauty', 'Toys', 'Automotive', 'Health', 'Food & Beverages'
]

PRODUCTS = {
    'Electronics': ['Smartphone', 'Laptop', 'Tablet', 'Headphones', 'Smart Watch', 'Camera', 'Gaming Console'],
    'Clothing': ['T-Shirt', 'Jeans', 'Dress', 'Sneakers', 'Jacket', 'Sweater', 'Pants'],
    'Home & Garden': ['Furniture', 'Kitchen Appliances', 'Garden Tools', 'Lighting', 'Bedding', 'Decor'],
    'Sports': ['Running Shoes', 'Fitness Equipment', 'Sports Apparel', 'Outdoor Gear', 'Bicycle'],
    'Books': ['Fiction Novel', 'Non-Fiction', 'Textbook', 'Cookbook', 'Biography', 'Self-Help'],
    'Beauty': ['Skincare', 'Makeup', 'Hair Care', 'Fragrance', 'Nail Care'],
    'Toys': ['Action Figures', 'Board Games', 'Educational Toys', 'Dolls', 'Building Blocks'],
    'Automotive': ['Car Parts', 'Car Accessories', 'Motor Oil', 'Tires', 'Car Electronics'],
    'Health': ['Vitamins', 'Medical Supplies', 'Fitness Supplements', 'Personal Care'],
    'Food & Beverages': ['Snacks', 'Beverages', 'Canned Goods', 'Fresh Produce', 'Frozen Foods']
}


def generate_batch_data(batch_size=100000):
    """Generate a batch of customer order data"""
    
    data = []
    
    for _ in range(batch_size):
        # Basic order info
        order_id = fake.uuid4()
        customer_id = fake.uuid4()
        
        # Customer info
        customer_name = fake.name()
        customer_email = fake.email()
        customer_phone = fake.phone_number()
        
        # Order timing
        order_date = fake.date_between(start_date='-2y', end_date='today')
        
        # Product info
        category = random.choice(PRODUCT_CATEGORIES)
        product_name = random.choice(PRODUCTS[category])
        product_id = fake.uuid4()
        
        # Order details
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(5.0, 500.0), 2)
        total_amount = round(quantity * unit_price, 2)
        
        # Shipping info
        shipping_address = fake.address().replace('\n', ', ')
        shipping_city = fake.city()
        shipping_state = fake.state()
        shipping_zip = fake.zipcode()
        shipping_country = fake.country()
        
        # Additional fields for data volume
        order_status = random.choice(['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled'])
        payment_method = random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Cash'])
        discount_amount = round(random.uniform(0, total_amount * 0.2), 2)
        tax_amount = round(total_amount * 0.08, 2)
        final_amount = round(total_amount - discount_amount + tax_amount, 2)
        
        # Store additional metadata
        store_id = random.randint(1000, 9999)
        sales_rep_id = random.randint(100, 999)
        promotion_code = fake.lexify(text='PROMO????') if random.random() > 0.7 else None
        
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
    
    return data


def create_parquet_file(file_number, target_size_gb=10, output_dir='data'):
    """Create a single parquet file with approximately target_size_gb of data"""
    
    print(f"Generating file {file_number}/10...")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    filename = os.path.join(output_dir, f'orders_part_{file_number:02d}.parquet')
    
    # Estimate rows needed (rough calculation)
    # Each row is approximately 1KB, so for 10GB we need about 10M rows
    estimated_rows_per_gb = 1000000  # 1M rows per GB (rough estimate)
    target_rows = target_size_gb * estimated_rows_per_gb
    
    batch_size = 100000  # Process in batches to manage memory
    total_batches = target_rows // batch_size
    
    # Create the file in batches
    first_batch = True
    current_rows = 0
    
    print(f"Target rows: {target_rows:,}")
    
    with tqdm(total=total_batches, desc=f"Creating file {file_number}") as pbar:
        for batch_num in range(total_batches):
            batch_data = generate_batch_data(batch_size)
            df_batch = pd.DataFrame(batch_data)
            
            # Convert to PyArrow Table for better performance
            table = pa.Table.from_pandas(df_batch)
            
            if first_batch:
                # Create new file
                pq.write_table(table, filename)
                first_batch = False
            else:
                # Append to existing file
                existing_table = pq.read_table(filename)
                combined_table = pa.concat_tables([existing_table, table])
                pq.write_table(combined_table, filename)
            
            current_rows += batch_size
            pbar.update(1)
            
            # Check file size periodically
            if batch_num % 10 == 0 and os.path.exists(filename):
                current_size_gb = os.path.getsize(filename) / (1024**3)
                pbar.set_postfix({'Size': f'{current_size_gb:.2f}GB', 'Rows': f'{current_rows:,}'})
                
                # If we've reached target size, break early
                if current_size_gb >= target_size_gb:
                    print(f"Reached target size of {target_size_gb}GB, stopping at {current_size_gb:.2f}GB")
                    break
    
    # Get final statistics
    final_size_gb = os.path.getsize(filename) / (1024**3)
    final_table = pq.read_table(filename)
    final_rows = len(final_table)
    
    print(f"File {file_number} completed:")
    print(f"  - Size: {final_size_gb:.2f} GB")
    print(f"  - Rows: {final_rows:,}")
    print(f"  - Location: {filename}")
    print()
    
    return filename, final_size_gb, final_rows


def main():
    """Main function to generate all sample data files"""
    
    print("Starting data generation for PySpark application...")
    print("This will create 10 parquet files, each approximately 10GB")
    print("=" * 60)
    
    start_time = datetime.now()
    total_size = 0
    total_rows = 0
    
    # Create output directory
    output_dir = 'data'
    os.makedirs(output_dir, exist_ok=True)
    
    files_info = []
    
    for i in range(1, 11):
        try:
            filename, size_gb, rows = create_parquet_file(i, target_size_gb=10, output_dir=output_dir)
            files_info.append({
                'file': filename,
                'size_gb': size_gb,
                'rows': rows
            })
            total_size += size_gb
            total_rows += rows
            
        except Exception as e:
            print(f"Error creating file {i}: {e}")
            sys.exit(1)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("=" * 60)
    print("DATA GENERATION COMPLETE!")
    print("=" * 60)
    print(f"Total files created: {len(files_info)}")
    print(f"Total size: {total_size:.2f} GB")
    print(f"Total rows: {total_rows:,}")
    print(f"Generation time: {duration}")
    print()
    
    print("File Summary:")
    for info in files_info:
        print(f"  {os.path.basename(info['file'])}: {info['size_gb']:.2f}GB, {info['rows']:,} rows")
    
    print(f"\nFiles are stored in: {os.path.abspath(output_dir)}")
    print("You can now run the PySpark application to process these files.")


if __name__ == "__main__":
    main()
