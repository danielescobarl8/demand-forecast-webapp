import streamlit as st
import pandas as pd
import numpy as np
import pdfkit
from io import BytesIO

def load_data(file, file_type):
    """Load Demand Forecast (Excel) or Data Feed (Excel or TXT)."""
    if file_type == 'excel':
        return pd.ExcelFile(file)
    elif file_type == 'txt':
        return pd.read_csv(file, delimiter='|', dtype=str)

def parse_month_column(col):
    """Convert month column names like 'feb-25' and '1/02/2025' to datetime format."""
    try:
        return pd.to_datetime(col, format='%b-%y', errors='coerce')  # Format like 'feb-25'
    except:
        try:
            return pd.to_datetime(col, format='%d/%m/%Y', errors='coerce')  # Format like '1/02/2025'
        except:
            return None

def process_data(demand_forecast, data_feed, country, month):
    """Process and clean the data, apply transformations, and merge datasets."""
    # Load sheets
    df_forecast = demand_forecast.parse(demand_forecast.sheet_names[0])
    
    # Ensure correct data types
    df_forecast['Product ID (PID)'] = df_forecast['Product ID (PID)'].astype(str)
    data_feed['MPL_PRODUCT_ID'] = data_feed['MPL_PRODUCT_ID'].astype(str)
    
    # Filter by country
    df_filtered = df_forecast[df_forecast['Market'] == country]
    
    # Convert month columns to datetime format
    month_column_mapping = {col: parse_month_column(col) for col in df_filtered.columns if isinstance(col, str)}
    df_filtered.rename(columns=month_column_mapping, inplace=True)
    
    # Get correct month format dynamically
    available_months = [col for col in df_filtered.columns if isinstance(col, pd.Timestamp)]
    
    if not available_months:
        raise ValueError("No valid month columns found in the dataset. Please check the file format.")
    
    selected_month = min(available_months, key=lambda x: abs(pd.to_datetime(x) - pd.to_datetime(month)))
    
    # Find top 20 PIDs by quantity
    top_20_pids_df = df_filtered[['Product ID (PID)', selected_month]].sort_values(by=selected_month, ascending=False).head(20)
    
    # Calculate % of Total
    total_quantity = top_20_pids_df[selected_month].sum()
    top_20_pids_df['% of Total'] = (top_20_pids_df[selected_month] / total_quantity * 100).round(0).astype(int).astype(str) + '%'
    
    # Match PIDs with URLs
    data_feed_links = data_feed[['MPL_PRODUCT_ID', 'LINK']].dropna()
    data_feed_links.rename(columns={'MPL_PRODUCT_ID': 'Product ID (PID)'}, inplace=True)
    
    # Handle cases where PIDs have extra leading numbers
    pid_map = {pid: mpl for pid in top_20_pids_df['Product ID (PID)'] for mpl in data_feed_links['Product ID (PID)'] if mpl.endswith(pid)}
    top_20_pids_df['Product ID (PID)'] = top_20_pids_df['Product ID (PID)'].replace(pid_map)
    
    # Merge URLs
    top_20_pids_with_links = top_20_pids_df.merge(data_feed_links.drop_duplicates('Product ID (PID)'), on='Product ID (PID)', how='left')
    
    return top_20_pids_with_links, selected_month

def generate_pdf(df, month):
    """Generate a PDF file with clickable links."""
    html_content = """
    <html>
    <head>
    <style>
    table { width: 100%; border-collapse: collapse; }
    th, td { border: 1px solid black; padding: 8px; text-align: left; }
    </style>
    </head>
    <body>
    <h2>Top 20 Products for {month}</h2>
    <table>
    <tr><th>Product ID</th><th>Quantity</th><th>% of Total</th><th>Link</th></tr>
    """.format(month=month.strftime('%B %Y'))
    
    for _, row in df.iterrows():
        link = f'<a href="{row["LINK"]}" target="_blank">View Product</a>' if pd.notna(row['LINK']) else 'N/A'
        html_content += f"<tr><td>{row['Product ID (PID)']}</td><td>{row[selected_month]}</td><td>{row['% of Total']}</td><td>{link}</td></tr>"
    
    html_content += """</table></body></html>"""
    
    # Convert HTML to PDF
    pdf_file = BytesIO()
    pdfkit.from_string(html_content, pdf_file, options={'quiet': ''})
    return pdf_file

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
        
        # Convert month columns
        df_forecast.rename(columns={col: parse_month_column(col) for col in df_forecast.columns if isinstance(col, str)}, inplace=True)
        available_months = [col for col in df_forecast.columns if isinstance(col, pd.Timestamp)]
        
        if not available_months:
            st.error("No valid month columns found. Please check the dataset.")
            return
        
        available_years = sorted(set(m.year for m in available_months))
        selected_year = st.selectbox("Select Year", available_years, index=0 if available_years else None)
        available_month_choices = sorted(set(m.month for m in available_months if m.year == selected_year))
        selected_month_num = st.selectbox("Select Month", available_month_choices, format_func=lambda x: pd.to_datetime(f'2025-{x}-01').strftime('%B'), index=0 if available_month_choices else None)
        selected_month = pd.to_datetime(f'{selected_year}-{selected_month_num}-01')
        
        if st.button("Process Data"):
            processed_data, selected_month = process_data(demand_forecast, data_feed, country, selected_month)
            st.write("### Processed Data")
            st.dataframe(processed_data)
            
            pdf_file = generate_pdf(processed_data, selected_month)
            st.download_button("Download PDF", pdf_file, "Processed_Data.pdf", "application/pdf")
            
if __name__ == "__main__":
    main()
