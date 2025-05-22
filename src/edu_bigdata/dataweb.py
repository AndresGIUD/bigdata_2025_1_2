import requests
import pandas as pd
import time
from datetime import datetime
import os

def fetch_spotify_data(url):
    """Fetch historical stock data from Investing.com."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }
    
    # Añadir retraso para evitar sobrecargar el servidor
    time.sleep(2)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tables = pd.read_html(response.text)
        
        # The main table is typically the first one
        df = tables[0]
        
        # Rename columns to match database schema
        df.columns = ['date', 'close_price', 'open_price', 'high_price', 'low_price', 'volume', 'change_percent']
        
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def clean_data(df):
    """Clean and transform the fetched data."""
    if df is None:
        return None
    
    # Copy dataframe to avoid modifying the original
    df_clean = df.copy()
    
    # Convert date to proper format (e.g., '28/10/2025' to '2025-10-28')
    df_clean['date'] = pd.to_datetime(df_clean['date'], format='%d/%m/%Y')
    
    # Clean volume (e.g., '1.23M' or '123K' to integer)
    def clean_volume(vol):
        if isinstance(vol, str):
            vol = vol.replace(',', '')
            if 'M' in vol:
                return int(float(vol.replace('M', '')) * 1_000_000)
            elif 'K' in vol:
                return int(float(vol.replace('K', '')) * 1_000)
        return int(vol)
    
    df_clean['volume'] = df_clean['volume'].apply(clean_volume)
    
    # Clean change_percent (e.g., '+1.23%' to 1.23)
    df_clean['change_percent'] = df_clean['change_percent'].str.replace('%', '').astype(float)
    
    # Ensure numeric columns
    numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price']
    for col in numeric_cols:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    return df_clean

def save_data(df, raw_path, processed_path):
    """Save raw and processed data to CSV files."""
    if df is not None:
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        df.to_csv(raw_path, index=False)
        clean_df = clean_data(df)
        if clean_df is not None:
            clean_df.to_csv(processed_path, index=False)
        return clean_df
    return None

if __name__ == "__main__":
    url = "https://es.investing.com/equities/spotify-technology-historical-data"
    raw_path = "data/raw_spotify_data.csv"
    processed_path = "data/processed_spotify_data.csv"
    
    df = fetch_spotify_data(url)
    clean_df = save_data(df, raw_path, processed_path)
    if clean_df is not None:
        print("Data fetched and saved successfully.")
    else:
        print("Failed to fetch or process data.")