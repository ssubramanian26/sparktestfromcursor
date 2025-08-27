#!/usr/bin/env python3
"""
Streamlit Runner Script
Launches the Streamlit web application for orders data visualization.
"""

import subprocess
import sys
import os
import time
import webbrowser
from pathlib import Path


def check_streamlit_installed():
    """Check if Streamlit is installed"""
    try:
        import streamlit
        return True
    except ImportError:
        return False


def check_data_available():
    """Check if any data files are available"""
    data_dirs = ['data', 'test_data']
    
    for directory in data_dirs:
        if os.path.exists(directory):
            parquet_files = [f for f in os.listdir(directory) if f.endswith('.parquet')]
            if parquet_files:
                return True, directory, len(parquet_files)
    
    return False, None, 0


def create_test_data():
    """Create small test data if no data exists"""
    print("No data found. Creating small test dataset...")
    
    try:
        # Run the quick test to create sample data
        result = subprocess.run([sys.executable, 'quick_test.py'], 
                              capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✅ Test data created successfully!")
            return True
        else:
            print(f"❌ Failed to create test data: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Test data creation timed out")
        return False
    except Exception as e:
        print(f"❌ Error creating test data: {e}")
        return False


def main():
    """Main function to run Streamlit application"""
    
    print("🚀 Starting Streamlit Orders Data Explorer...")
    print("=" * 50)
    
    # Check if Streamlit is installed
    if not check_streamlit_installed():
        print("❌ Streamlit is not installed!")
        print("Please install it with: pip install streamlit plotly")
        return False
    
    print("✅ Streamlit is installed")
    
    # Check if data is available
    data_available, data_dir, file_count = check_data_available()
    
    if not data_available:
        print("⚠️  No parquet data files found")
        create_test = input("Create small test dataset? (y/N): ").lower().startswith('y')
        
        if create_test:
            if not create_test_data():
                print("\nTo manually create data, run:")
                print("1. python3 quick_test.py  # For small test data")
                print("2. python3 generate_sample_data.py  # For full dataset")
                return False
        else:
            print("\nTo create data, run:")
            print("1. python3 quick_test.py  # For small test data")
            print("2. python3 generate_sample_data.py  # For full dataset")
            return False
    else:
        print(f"✅ Found {file_count} parquet files in '{data_dir}' directory")
    
    # Launch Streamlit
    print("\n🌟 Launching Streamlit application...")
    print("The app will open in your default web browser")
    print("If it doesn't open automatically, go to: http://localhost:8501")
    print("\nPress Ctrl+C to stop the server")
    print("=" * 50)
    
    try:
        # Launch Streamlit
        subprocess.run([
            sys.executable, '-m', 'streamlit', 'run', 
            'streamlit_app.py',
            '--server.address', 'localhost',
            '--server.port', '8501',
            '--browser.gatherUsageStats', 'false'
        ])
        
    except KeyboardInterrupt:
        print("\n👋 Streamlit application stopped by user")
        return True
    except Exception as e:
        print(f"❌ Error running Streamlit: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
