import sqlite3
import pandas as pd
import os

def create_database(db_path):
    """Create SQLite database and table for Spotify stock data."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Crear tabla
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS spotify_stock_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            volume INTEGER,
            change_percent REAL
        )
    ''')
    
    conn.commit()
    conn.close()

def insert_data(db_path, processed_path):
    """Insert processed data into the SQLite database."""
    if not os.path.exists(processed_path):
        print(f"Processed data file {processed_path} not found.")
        return
    
    df = pd.read_csv(processed_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Insertar datos
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO spotify_stock_data (date, open_price, high_price, low_price, close_price, volume, change_percent)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['date'],
            row['open_price'],
            row['high_price'],
            row['low_price'],
            row['close_price'],
            row['volume'],
            row['change_percent']
        ))
    
    conn.commit()
    conn.close()
    print("Data inserted into database successfully.")

if __name__ == "__main__":
    db_path = "db/spotify_stock.db"
    processed_path = "data/processed_spotify_data.csv"
    
    create_database(db_path)
    insert_data(db_path, processed_path)