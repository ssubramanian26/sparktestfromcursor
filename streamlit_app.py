#!/usr/bin/env python3
"""
Streamlit Web Application for PySpark Orders Data Visualization
Interactive dashboard to explore and filter customer orders data.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import glob
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa


# Configure Streamlit page
st.set_page_config(
    page_title="Orders Data Explorer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_data
def load_data(data_directory="data", max_rows=None):
    """Load parquet data with caching for better performance"""
    
    # Look for parquet files in both 'data' and 'test_data' directories
    possible_dirs = [data_directory, 'test_data']
    parquet_files = []
    
    for directory in possible_dirs:
        if os.path.exists(directory):
            files = glob.glob(os.path.join(directory, "*.parquet"))
            parquet_files.extend(files)
    
    if not parquet_files:
        return None, "No parquet files found. Please generate data first."
    
    try:
        # Read all parquet files
        tables = []
        for file in parquet_files:
            table = pq.read_table(file)
            tables.append(table)
        
        # Combine all tables
        combined_table = pa.concat_tables(tables)
        df = combined_table.to_pandas()
        
        # Limit rows if specified
        if max_rows and len(df) > max_rows:
            df = df.head(max_rows)
        
        # Convert date columns
        if 'order_date' in df.columns:
            df['order_date'] = pd.to_datetime(df['order_date'])
        
        return df, None
        
    except Exception as e:
        return None, f"Error loading data: {str(e)}"


def create_overview_metrics(df):
    """Create overview metrics cards"""
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Orders",
            value=f"{len(df):,}",
            delta=None
        )
    
    with col2:
        total_revenue = df['final_amount'].sum()
        st.metric(
            label="Total Revenue",
            value=f"${total_revenue:,.2f}",
            delta=None
        )
    
    with col3:
        avg_order_value = df['final_amount'].mean()
        st.metric(
            label="Avg Order Value",
            value=f"${avg_order_value:.2f}",
            delta=None
        )
    
    with col4:
        unique_customers = df['customer_id'].nunique()
        st.metric(
            label="Unique Customers",
            value=f"{unique_customers:,}",
            delta=None
        )


def create_filters(df):
    """Create sidebar filters for the data"""
    
    st.sidebar.header("🔍 Data Filters")
    
    # Row count filter
    max_available_rows = len(df)
    row_count = st.sidebar.slider(
        "Number of rows to display",
        min_value=10,
        max_value=min(max_available_rows, 10000),  # Cap at 10k for performance
        value=min(1000, max_available_rows),
        step=10,
        help="Select how many rows to display (capped at 10,000 for performance)"
    )
    
    # Date range filter
    if 'order_date' in df.columns and not df['order_date'].isna().all():
        min_date = df['order_date'].min().date()
        max_date = df['order_date'].max().date()
        
        date_range = st.sidebar.date_input(
            "Order Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Filter orders by date range"
        )
    else:
        date_range = None
    
    # Product category filter
    if 'product_category' in df.columns:
        categories = sorted(df['product_category'].unique())
        selected_categories = st.sidebar.multiselect(
            "Product Categories",
            options=categories,
            default=categories,
            help="Select product categories to include"
        )
    else:
        selected_categories = None
    
    # Order status filter
    if 'order_status' in df.columns:
        statuses = sorted(df['order_status'].unique())
        selected_statuses = st.sidebar.multiselect(
            "Order Status",
            options=statuses,
            default=statuses,
            help="Select order statuses to include"
        )
    else:
        selected_statuses = None
    
    # Payment method filter
    if 'payment_method' in df.columns:
        payment_methods = sorted(df['payment_method'].unique())
        selected_payment_methods = st.sidebar.multiselect(
            "Payment Methods",
            options=payment_methods,
            default=payment_methods,
            help="Select payment methods to include"
        )
    else:
        selected_payment_methods = None
    
    # Revenue range filter
    if 'final_amount' in df.columns:
        min_revenue = float(df['final_amount'].min())
        max_revenue = float(df['final_amount'].max())
        
        revenue_range = st.sidebar.slider(
            "Revenue Range ($)",
            min_value=min_revenue,
            max_value=max_revenue,
            value=(min_revenue, max_revenue),
            step=1.0,
            help="Filter orders by revenue amount"
        )
    else:
        revenue_range = None
    
    return {
        'row_count': row_count,
        'date_range': date_range,
        'selected_categories': selected_categories,
        'selected_statuses': selected_statuses,
        'selected_payment_methods': selected_payment_methods,
        'revenue_range': revenue_range
    }


def apply_filters(df, filters):
    """Apply selected filters to the dataframe"""
    
    filtered_df = df.copy()
    
    # Apply date filter
    if filters['date_range'] and len(filters['date_range']) == 2:
        start_date, end_date = filters['date_range']
        if 'order_date' in filtered_df.columns:
            filtered_df = filtered_df[
                (filtered_df['order_date'].dt.date >= start_date) &
                (filtered_df['order_date'].dt.date <= end_date)
            ]
    
    # Apply category filter
    if filters['selected_categories'] and 'product_category' in filtered_df.columns:
        filtered_df = filtered_df[
            filtered_df['product_category'].isin(filters['selected_categories'])
        ]
    
    # Apply status filter
    if filters['selected_statuses'] and 'order_status' in filtered_df.columns:
        filtered_df = filtered_df[
            filtered_df['order_status'].isin(filters['selected_statuses'])
        ]
    
    # Apply payment method filter
    if filters['selected_payment_methods'] and 'payment_method' in filtered_df.columns:
        filtered_df = filtered_df[
            filtered_df['payment_method'].isin(filters['selected_payment_methods'])
        ]
    
    # Apply revenue filter
    if filters['revenue_range'] and 'final_amount' in filtered_df.columns:
        min_rev, max_rev = filters['revenue_range']
        filtered_df = filtered_df[
            (filtered_df['final_amount'] >= min_rev) &
            (filtered_df['final_amount'] <= max_rev)
        ]
    
    # Apply row count limit
    filtered_df = filtered_df.head(filters['row_count'])
    
    return filtered_df


def create_visualizations(df):
    """Create interactive visualizations"""
    
    st.header("📈 Data Visualizations")
    
    # Create tabs for different visualizations
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Sales Analysis", "🛍️ Product Analysis", "📅 Time Analysis", "🌍 Geographic Analysis"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            # Orders by Status
            if 'order_status' in df.columns:
                status_counts = df['order_status'].value_counts()
                fig_status = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title="Orders by Status"
                )
                fig_status.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_status, width='stretch')
        
        with col2:
            # Payment Methods
            if 'payment_method' in df.columns:
                payment_counts = df['payment_method'].value_counts()
                fig_payment = px.bar(
                    x=payment_counts.values,
                    y=payment_counts.index,
                    orientation='h',
                    title="Orders by Payment Method"
                )
                fig_payment.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_payment, width='stretch')
        
        # Revenue Distribution
        if 'final_amount' in df.columns:
            fig_revenue = px.histogram(
                df,
                x='final_amount',
                nbins=50,
                title="Revenue Distribution",
                labels={'final_amount': 'Order Value ($)', 'count': 'Number of Orders'}
            )
            st.plotly_chart(fig_revenue, width='stretch')
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            # Product Categories
            if 'product_category' in df.columns:
                category_revenue = df.groupby('product_category')['final_amount'].sum().sort_values(ascending=True)
                fig_category = px.bar(
                    x=category_revenue.values,
                    y=category_revenue.index,
                    orientation='h',
                    title="Revenue by Product Category"
                )
                fig_category.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_category, width='stretch')
        
        with col2:
            # Top Products
            if 'product_name' in df.columns:
                top_products = df.groupby('product_name')['final_amount'].sum().nlargest(10)
                fig_products = px.bar(
                    x=top_products.values,
                    y=top_products.index,
                    orientation='h',
                    title="Top 10 Products by Revenue"
                )
                fig_products.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_products, width='stretch')
    
    with tab3:
        # Time-based analysis
        if 'order_date' in df.columns:
            # Daily orders
            daily_orders = df.groupby(df['order_date'].dt.date).agg({
                'order_id': 'count',
                'final_amount': 'sum'
            }).reset_index()
            daily_orders.columns = ['date', 'order_count', 'revenue']
            
            fig_time = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add order count
            fig_time.add_trace(
                go.Scatter(x=daily_orders['date'], y=daily_orders['order_count'], name="Order Count"),
                secondary_y=False,
            )
            
            # Add revenue
            fig_time.add_trace(
                go.Scatter(x=daily_orders['date'], y=daily_orders['revenue'], name="Revenue ($)", line=dict(color='red')),
                secondary_y=True,
            )
            
            fig_time.update_xaxes(title_text="Date")
            fig_time.update_yaxes(title_text="Order Count", secondary_y=False)
            fig_time.update_yaxes(title_text="Revenue ($)", secondary_y=True)
            fig_time.update_layout(title_text="Daily Orders and Revenue Over Time")
            
            st.plotly_chart(fig_time, width='stretch')
    
    with tab4:
        # Geographic analysis
        if 'shipping_state' in df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Top states by orders
                top_states = df.groupby('shipping_state').agg({
                    'order_id': 'count',
                    'final_amount': 'sum'
                }).sort_values('order_id', ascending=False).head(15)
                
                fig_states = px.bar(
                    x=top_states['order_id'],
                    y=top_states.index,
                    orientation='h',
                    title="Top 15 States by Order Count"
                )
                fig_states.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_states, width='stretch')
            
            with col2:
                # Revenue by state
                state_revenue = df.groupby('shipping_state')['final_amount'].sum().sort_values(ascending=False).head(15)
                
                fig_state_rev = px.bar(
                    x=state_revenue.values,
                    y=state_revenue.index,
                    orientation='h',
                    title="Top 15 States by Revenue"
                )
                fig_state_rev.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_state_rev, width='stretch')



def display_data_table(df):
    """Display the filtered data table"""
    
    st.header("📋 Filtered Data Table")
    
    # Add column selection
    all_columns = list(df.columns)
    selected_columns = st.multiselect(
        "Select columns to display",
        options=all_columns,
        default=all_columns[:10] if len(all_columns) > 10 else all_columns,
        help="Choose which columns to show in the table"
    )
    
    if selected_columns:
        display_df = df[selected_columns]
        
        # Display the table
        st.dataframe(
            display_df,
            width='stretch',
            height=400
        )
        
        # Download button
        csv = display_df.to_csv(index=False)
        st.download_button(
            label="📥 Download filtered data as CSV",
            data=csv,
            file_name=f"filtered_orders_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )


def main():
    """Main Streamlit application"""
    
    # Title and description
    st.title("📊 Orders Data Explorer")
    st.markdown("""
    Interactive dashboard to explore and analyze customer orders data from your PySpark application.
    Use the filters in the sidebar to customize your view.
    """)
    
    # Data loading section
    with st.spinner("🔄 Loading data..."):
        df, error = load_data(max_rows=50000)  # Limit for performance
    
    if error:
        st.error(f"❌ {error}")
        st.info("""
        **To get started:**
        1. Run `python3 generate_sample_data.py` to create full dataset, OR
        2. Run `python3 quick_test.py` to create small test dataset
        3. Refresh this page
        """)
        return
    
    if df is None or len(df) == 0:
        st.warning("No data available to display.")
        return
    
    st.success(f"✅ Data loaded successfully! {len(df):,} rows available")
    
    # Create filters
    filters = create_filters(df)
    
    # Apply filters
    filtered_df = apply_filters(df, filters)
    
    if len(filtered_df) == 0:
        st.warning("⚠️ No data matches the selected filters. Please adjust your filter criteria.")
        return
    
    # Display metrics
    create_overview_metrics(filtered_df)
    
    # Add separator
    st.markdown("---")
    
    # Create visualizations
    create_visualizations(filtered_df)
    
    # Add separator
    st.markdown("---")
    
    # Display data table
    display_data_table(filtered_df)
    
    # Footer
    st.markdown("---")
    st.markdown("**📈 Built with Streamlit | Powered by PySpark Orders Processing Application**")


if __name__ == "__main__":
    main()
