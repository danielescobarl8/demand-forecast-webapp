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

def detect_delimiter(uploaded_file):
    """Automatically detect the delimiter in an uploaded file."""
    first_line = uploaded_file.getvalue().decode("utf-8").split("\n")[0]
    for char in ['|', '\t', ',']:
        if char in first_line:
            return char
    return ','  # Default fallback

def process_data(demand_forecast, data_feed, country, selected_column):
    """Process and clean the data, apply transformations, and merge datasets."""
    # Load sheets
    df_forecast = demand_forecast.parse(demand_forecast.sheet_names[0])
    
    # Strip column names and ensure all are strings
    df_forecast.columns = df_forecast.columns.astype(str).str.strip()
    selected_column = str(selected_column)  # Ensure selected column is also a string
    
    # Filter by country
    df_filtered = df_forecast[df_forecast['Market'] == country]
    
    # Ensure the selected column is numeric
    if selected_column in df_filtered.columns:
        df_filtered[selected_column] = pd.to_numeric(df_filtered[selected_column], errors='coerce').fillna(0)
    else:
        raise KeyError(f"Selected column '{selected_column}' not found in DataFrame.")
    
    # Find top 20 PIDs by Total Revenue at PVP
    top_20_pids_df = df_filtered[['Product ID (PID)', selected_column]].copy()
    
    # Ensure data types for merging
    data_feed['MPL_PRODUCT_ID'] = data_feed['MPL_PRODUCT_ID'].astype(str)
    top_20_pids_df['Product ID (PID)'] = top_20_pids_df['Product ID (PID)'].astype(str)
    
    # Extract MPL_PRODUCT_ID, LINK, and CONSUMERPRICE from data feed
    data_feed_info = data_feed[['MPL_PRODUCT_ID', 'LINK', 'CONSUMERPRICE']].dropna()
    data_feed_info['CONSUMERPRICE'] = pd.to_numeric(data_feed_info['CONSUMERPRICE'], errors='coerce').fillna(0)
    
    # Handle cases where PIDs have extra leading numbers
    pid_map = {
        pid: mpl 
        for pid in top_20_pids_df['Product ID (PID)'] 
        for mpl in data_feed_info['MPL_PRODUCT_ID'] 
        if mpl.endswith(pid) or mpl[-len(pid):] == pid
    }
    top_20_pids_df['Product ID (PID)'] = top_20_pids_df['Product ID (PID)'].replace(pid_map)
    
    # Merge with data feed to get URLs and CONSUMERPRICE
    top_20_pids_with_info = top_20_pids_df.merge(data_feed_info.drop_duplicates('MPL_PRODUCT_ID'), left_on="Product ID (PID)", right_on="MPL_PRODUCT_ID", how="left").drop(columns=["MPL_PRODUCT_ID"])
    
    # Calculate Total Revenue at PVP
    top_20_pids_with_info['Total Revenue at PVP'] = top_20_pids_with_info[selected_column] * top_20_pids_with_info['CONSUMERPRICE']
    
    # Format currency columns
    top_20_pids_with_info['CONSUMERPRICE'] = top_20_pids_with_info['CONSUMERPRICE'].apply(lambda x: f"${x:,.2f}")
    top_20_pids_with_info['Total Revenue at PVP'] = top_20_pids_with_info['Total Revenue at PVP'].apply(lambda x: f"${x:,.2f}")
    
    # Sort by Total Revenue at PVP
    top_20_pids_with_info = top_20_pids_with_info.sort_values(by='Total Revenue at PVP', ascending=False).head(20)
    
    return top_20_pids_with_info, selected_column

def generate_excel(dataframe):
    """Generate an Excel file from the processed data."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        dataframe.to_excel(writer, sheet_name='Sheet1', index=False)
    output.seek(0)
    return output

def main():
    st.title("Demand Forecast & Data Feed Processor")
    
    # File Upload
    demand_file = st.file_uploader("Upload Demand Forecast (Excel)", type=['xlsx'])
    data_file = st.file_uploader("Upload Data Feed (Excel or TXT)", type=['xlsx', 'txt'])
    
    if demand_file and data_file:
        # Load files
        demand_forecast = load_data(demand_file, 'excel')
        data_feed = load_data(data_file, 'txt' if data_file.name.endswith('.txt') else 'excel')
        
        # Select Country
        df_forecast = demand_forecast.parse(demand_forecast.sheet_names[0])
        df_forecast.columns = df_forecast.columns.astype(str).str.strip()
        available_countries = df_forecast['Market'].dropna().unique()
        country = st.selectbox("Select Country", available_countries)
        
        # Identify selectable columns (starting from column T)
        selectable_columns = df_forecast.columns[19:].tolist()
        selected_column = st.selectbox("Select Month Column", selectable_columns)
        
        if st.button("Process Data"):
            try:
                processed_data, selected_column = process_data(demand_forecast, data_feed, country, selected_column)
                st.write("### Processed Data")
                st.dataframe(processed_data)
                
                # Generate and allow Excel download
                excel_file = generate_excel(processed_data)
                st.download_button("Download Excel", excel_file, "Demand Forecast Website Readiness.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except KeyError as e:
                st.error(f"Error: {e}")
            
if __name__ == "__main__":
    main()
