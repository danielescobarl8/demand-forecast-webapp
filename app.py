import streamlit as st
import pandas as pd
import numpy as np
import pdfkit
from io import BytesIO
import csv

def load_data(file, file_type):
    """Load Demand Forecast (Excel) or Data Feed (Excel or TXT)."""
    if file_type == 'excel':
        return pd.ExcelFile(file)
    elif file_type == 'txt':
        return pd.read_csv(file, delimiter=detect_delimiter(file), dtype=str)

def detect_delimiter(file_path):
    """Automatically detect the delimiter in a file."""
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        sniffer = csv.Sniffer()
        first_line = f.readline()
        if sniffer.has_header(first_line):
            first_line = f.readline()
        delimiter = ','  # Default fallback
        for char in ['|', '\t', ',']:
            if char in first_line:
                delimiter = char
                break
    return delimiter

def process_data(demand_forecast, data_feed, country, month):
    """Process and clean the data, apply transformations, and merge datasets."""
    # Load sheets
    df_forecast = demand_forecast.parse(demand_forecast.sheet_names[0])
    
    # Strip column names and convert month columns to datetime format
    df_forecast.columns = df_forecast.columns.astype(str).str.strip()
    df_forecast.columns = [pd.to_datetime(col, errors="coerce") if "2025" in str(col) or "2026" in str(col) else col for col in df_forecast.columns]
    
    # Identify available months
    available_months = [col for col in df_forecast.columns if isinstance(col, pd.Timestamp)]
    
    if not available_months:
        raise ValueError("No valid month columns found in the dataset. Please check the file format.")
    
    selected_month_col = min(available_months, key=lambda x: abs(x - month))
    
    # Filter by country
    df_filtered = df_forecast[df_forecast['Market'] == country]
    
    # Ensure the selected month column is numeric
    df_filtered[selected_month_col] = pd.to_numeric(df_filtered[selected_month_col], errors='coerce').fillna(0)
    
    # Find top 20 PIDs by quantity
    top_20_pids_df = df_filtered[['Product ID (PID)', selected_month_col]].sort_values(by=selected_month_col, ascending=False).head(20)
    
    # Calculate % of Total
    total_quantity = top_20_pids_df[selected_month_col].sum()
    top_20_pids_df['% of Total'] = ((top_20_pids_df[selected_month_col] / total_quantity) * 100).round(0).astype(int).astype(str) + '%'
    
    # Ensure data types for merging
    data_feed['MPL_PRODUCT_ID'] = data_feed['MPL_PRODUCT_ID'].astype(str)
    top_20_pids_df['Product ID (PID)'] = top_20_pids_df['Product ID (PID)'].astype(str)
    
    # Extract MPL_PRODUCT_ID and LINK from data feed
    data_feed_links = data_feed[['MPL_PRODUCT_ID', 'LINK']].dropna()
    
    # Handle cases where PIDs have extra leading numbers
    pid_map = {pid: mpl for pid in top_20_pids_df['Product ID (PID)'] for mpl in data_feed_links['MPL_PRODUCT_ID'] if mpl.endswith(pid)}
    top_20_pids_df['Product ID (PID)'] = top_20_pids_df['Product ID (PID)'].replace(pid_map)
    
    # Merge with data feed to get URLs
    top_20_pids_with_links = top_20_pids_df.merge(data_feed_links.drop_duplicates('MPL_PRODUCT_ID'), left_on="Product ID (PID)", right_on="MPL_PRODUCT_ID", how="left").drop(columns=["MPL_PRODUCT_ID"])
    
    return top_20_pids_with_links, selected_month_col

def main():
    st.title("Demand Forecast & Data Feed Processor")
    
    # File Upload
    demand_file = st.file_uploader("Upload Demand Forecast (Excel)", type=['xlsx'])
    data_file = st.file_uploader("Upload Data Feed (Excel or TXT)", type=['xlsx', 'txt'])
    
    if demand_file and data_file:
        # Load files
        demand_forecast = load_data(demand_file, 'excel')
        data_feed = load_data(data_file, 'txt' if data_file.name.endswith('.txt') else 'excel')
        
        # Select Country and Month
        df_forecast = demand_forecast.parse(demand_forecast.sheet_names[0])
        available_countries = df_forecast['Market'].dropna().unique()
        country = st.selectbox("Select Country", available_countries)
        
        available_months = [col for col in df_forecast.columns if isinstance(col, pd.Timestamp)]
        if not available_months:
            st.error("No valid month columns found. Please check the dataset.")
            return
        
        selected_month = pd.to_datetime(st.date_input("Select Month", value=pd.to_datetime(available_months[0])))
        
        if st.button("Process Data"):
            processed_data, selected_month_col = process_data(demand_forecast, data_feed, country, selected_month)
            st.write("### Processed Data")
            st.dataframe(processed_data)
            
            # Generate and allow PDF download
            pdf_file = generate_pdf(processed_data, selected_month_col)
            st.download_button("Download PDF", pdf_file, "Processed_Data.pdf", "application/pdf")
            
if __name__ == "__main__":
    main()
