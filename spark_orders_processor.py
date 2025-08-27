#!/usr/bin/env python3
"""
PySpark Orders Processing Application
Reads 10 large parquet files with customer order data and processes them to show insights.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
import time
from datetime import datetime


class OrdersProcessor:
    """Main class for processing customer orders data using PySpark"""
    
    def __init__(self, app_name="OrdersProcessor"):
        """Initialize Spark session with optimized configurations"""
        
        print("Initializing Spark session...")
        
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.parquet.cacheMetadata", "true") \
            .config("spark.sql.parquet.mergeSchema", "false") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.columnarReaderBatchSize", "4096") \
            .config("spark.driver.memory", "8g") \
            .config("spark.driver.maxResultSize", "4g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.cores", "4") \
            .config("spark.default.parallelism", "200") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
        
        # Set log level to reduce verbose output
        self.spark.sparkContext.setLogLevel("WARN")
        
        print(f"Spark session created successfully!")
        print(f"Spark version: {self.spark.version}")
        print(f"Application ID: {self.spark.sparkContext.applicationId}")
        print()
    
    def load_data(self, data_directory="data"):
        """Load all parquet files from the data directory"""
        
        print(f"Loading data from directory: {os.path.abspath(data_directory)}")
        
        # Check if directory exists
        if not os.path.exists(data_directory):
            print(f"Error: Data directory '{data_directory}' not found!")
            print("Please run the data generation script first: python generate_sample_data.py")
            sys.exit(1)
        
        # Find all parquet files
        parquet_files = [f for f in os.listdir(data_directory) if f.endswith('.parquet')]
        
        if not parquet_files:
            print(f"Error: No parquet files found in '{data_directory}'!")
            print("Please run the data generation script first: python generate_sample_data.py")
            sys.exit(1)
        
        print(f"Found {len(parquet_files)} parquet files:")
        for file in sorted(parquet_files):
            file_path = os.path.join(data_directory, file)
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"  - {file}: {size_mb:.2f} MB")
        
        print("\nLoading data into Spark DataFrame...")
        start_time = time.time()
        
        # Load all parquet files into a single DataFrame
        df = self.spark.read.parquet(f"{data_directory}/*.parquet")
        
        # Cache the DataFrame for better performance on multiple operations
        df.cache()
        
        # Trigger an action to actually load and cache the data
        total_rows = df.count()
        load_time = time.time() - start_time
        
        print(f"Data loaded successfully!")
        print(f"Total rows: {total_rows:,}")
        print(f"Load time: {load_time:.2f} seconds")
        print()
        
        return df
    
    def show_data_overview(self, df):
        """Display basic information about the loaded data"""
        
        print("=" * 60)
        print("DATA OVERVIEW")
        print("=" * 60)
        
        # Show schema
        print("Schema:")
        df.printSchema()
        
        # Show basic statistics
        print("\nDataset Statistics:")
        print(f"Total rows: {df.count():,}")
        print(f"Total columns: {len(df.columns)}")
        
        # Show sample data
        print("\nFirst 100 rows:")
        print("=" * 60)
        df.show(100, truncate=False)
        
        return df
    
    def analyze_data(self, df):
        """Perform some basic analysis on the orders data"""
        
        print("=" * 60)
        print("DATA ANALYSIS")
        print("=" * 60)
        
        # Analysis 1: Orders by status
        print("1. Orders by Status:")
        df.groupBy("order_status") \
          .agg(count("*").alias("order_count"),
               sum("final_amount").alias("total_revenue")) \
          .orderBy(desc("order_count")) \
          .show()
        
        # Analysis 2: Top product categories by revenue
        print("2. Top Product Categories by Revenue:")
        df.groupBy("product_category") \
          .agg(count("*").alias("order_count"),
               sum("final_amount").alias("total_revenue")) \
          .orderBy(desc("total_revenue")) \
          .show()
        
        # Analysis 3: Monthly order trends
        print("3. Monthly Order Trends:")
        df.withColumn("order_month", date_format(col("order_date"), "yyyy-MM")) \
          .groupBy("order_month") \
          .agg(count("*").alias("order_count"),
               sum("final_amount").alias("total_revenue")) \
          .orderBy("order_month") \
          .show()
        
        # Analysis 4: Payment method distribution
        print("4. Payment Method Distribution:")
        df.groupBy("payment_method") \
          .agg(count("*").alias("order_count"),
               avg("final_amount").alias("avg_order_value")) \
          .orderBy(desc("order_count")) \
          .show()
        
        # Analysis 5: Top states by orders
        print("5. Top States by Order Count:")
        df.groupBy("shipping_state") \
          .agg(count("*").alias("order_count")) \
          .orderBy(desc("order_count")) \
          .limit(20) \
          .show()
        
        return df
    
    def export_sample_data(self, df, output_path="output/sample_100_rows"):
        """Export the first 100 rows to a new parquet file"""
        
        print(f"Exporting first 100 rows to: {output_path}")
        
        # Create output directory
        os.makedirs("output", exist_ok=True)
        
        # Get first 100 rows and write to parquet
        sample_df = df.limit(100)
        sample_df.coalesce(1).write.mode("overwrite").parquet(output_path)
        
        print(f"Sample data exported successfully to: {os.path.abspath(output_path)}")
    
    def cleanup(self):
        """Clean up Spark session"""
        print("Cleaning up Spark session...")
        self.spark.stop()
        print("Spark session stopped.")


def main():
    """Main function to run the orders processing application"""
    
    print("=" * 80)
    print("PYSPARK ORDERS PROCESSING APPLICATION")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print()
    
    # Initialize processor
    processor = OrdersProcessor("CustomerOrdersAnalysis")
    
    try:
        # Load data
        df = processor.load_data("data")
        
        # Show data overview and first 100 rows
        df = processor.show_data_overview(df)
        
        # Perform analysis
        processor.analyze_data(df)
        
        # Export sample data
        processor.export_sample_data(df)
        
        print("\n" + "=" * 80)
        print("APPLICATION COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"Completed at: {datetime.now()}")
        
    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Clean up
        processor.cleanup()


if __name__ == "__main__":
    main()
