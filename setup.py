#!/usr/bin/env python3
"""
Setup and validation script for PySpark Orders Processing Application
"""

import sys
import subprocess
import os
from pathlib import Path


def check_python_version():
    """Check if Python version is 3.8 or higher"""
    if sys.version_info < (3, 8):
        print(f"❌ Python 3.8+ required. Current version: {sys.version}")
        return False
    print(f"✅ Python version: {sys.version.split()[0]}")
    return True


def check_java_installation():
    """Check if Java is installed and accessible"""
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            java_version = result.stderr.split('\n')[0] if result.stderr else "Unknown"
            print(f"✅ Java installed: {java_version}")
            return True
    except FileNotFoundError:
        pass
    
    print("❌ Java not found. Please install Java 8 or 11.")
    print("   macOS: brew install openjdk@11")
    print("   Linux: sudo apt install openjdk-11-jdk")
    return False


def check_dependencies():
    """Check if required Python packages are available"""
    required_packages = [
        'pyspark', 'pandas', 'numpy', 'pyarrow', 'faker', 'tqdm'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} installed")
        except ImportError:
            print(f"❌ {package} missing")
            missing_packages.append(package)
    
    return len(missing_packages) == 0, missing_packages


def test_spark_initialization():
    """Test if PySpark can initialize properly"""
    try:
        from pyspark.sql import SparkSession
        
        print("🧪 Testing Spark initialization...")
        spark = SparkSession.builder \
            .appName("SetupTest") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        
        # Test basic operation
        test_data = [(1, "test"), (2, "data")]
        df = spark.createDataFrame(test_data, ["id", "value"])
        count = df.count()
        
        spark.stop()
        
        if count == 2:
            print("✅ Spark initialization successful")
            return True
        else:
            print("❌ Spark test failed - unexpected result")
            return False
            
    except Exception as e:
        print(f"❌ Spark initialization failed: {e}")
        return False


def create_directories():
    """Create necessary directories"""
    directories = ['data', 'output']
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✅ Directory created/verified: {directory}")


def main():
    """Main setup and validation function"""
    print("=" * 60)
    print("PYSPARK ORDERS PROCESSING - SETUP VALIDATION")
    print("=" * 60)
    
    all_checks_passed = True
    
    # Check Python version
    print("\n1. Checking Python version...")
    if not check_python_version():
        all_checks_passed = False
    
    # Check Java installation
    print("\n2. Checking Java installation...")
    if not check_java_installation():
        all_checks_passed = False
    
    # Check dependencies
    print("\n3. Checking Python dependencies...")
    deps_ok, missing = check_dependencies()
    if not deps_ok:
        all_checks_passed = False
        print(f"\n   Install missing packages with:")
        print(f"   pip install {' '.join(missing)}")
    
    # Test Spark
    if deps_ok:
        print("\n4. Testing Spark initialization...")
        if not test_spark_initialization():
            all_checks_passed = False
    
    # Create directories
    print("\n5. Creating project directories...")
    create_directories()
    
    # Final status
    print("\n" + "=" * 60)
    if all_checks_passed:
        print("✅ SETUP VALIDATION PASSED!")
        print("\nYou can now run:")
        print("1. python generate_sample_data.py  # Generate sample data")
        print("2. python spark_orders_processor.py  # Process the data")
    else:
        print("❌ SETUP VALIDATION FAILED!")
        print("\nPlease fix the issues above before proceeding.")
    print("=" * 60)
    
    return all_checks_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
