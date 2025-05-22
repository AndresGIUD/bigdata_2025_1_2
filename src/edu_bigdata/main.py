from dataweb import fetch_spotify_data, save_data
from database import create_database, insert_data

def main():
    """Main ETL pipeline for Spotify stock data."""
    url = "https://es.investing.com/equities/spotify-technology-historical-data"
    raw_path = "data/raw_spotify_data.csv"
    processed_path = "data/processed_spotify_data.csv"
    db_path = "db/spotify_stock.db"
    
    # Para obtener y guardar datos
    df = fetch_spotify_data(url)
    if df is None:
        print("Failed to fetch data. Exiting.")
        return
    
    clean_df = save_data(df, raw_path, processed_path)
    if clean_df is None:
        print("Failed to process data. Exiting.")
        return
    
    # Crear base de datos e insertar datos
    create_database(db_path)
    insert_data(db_path, processed_path)
    
    print("ETL pipeline completed successfully.")

if __name__ == "__main__":
    main()