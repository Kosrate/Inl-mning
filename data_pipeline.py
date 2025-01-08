import pandas as pd
import sqlite3
import logging
from datetime import datetime

# Konfigurera loggning
logging.basicConfig(
    filename="pipeline.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def read_data(file_path):
    try:
        data = pd.read_csv(file_path)
        return data
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        raise

def process_data(df):
    try:
        # Exempel på bearbetning: konvertera datumformat
        df['processed_date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        # Lägg till en ny kolumn
        df['status'] = 'processed'
        return df
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

def update_database(df, db_path, table_name):
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
    except Exception as e:
        logging.error(f"Error updating database: {e}")
        raise

if __name__ == "__main__":
    try:
        # Läs in data
        file_path = "data.csv"
        db_path = "data_pipeline.db"
        table_name = "processed_data"

        data = read_data(file_path)
        processed_data = process_data(data)
        update_database(processed_data, db_path, table_name)

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")