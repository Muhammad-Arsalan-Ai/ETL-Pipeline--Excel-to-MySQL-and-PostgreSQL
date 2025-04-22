import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import json
import os
import logging
import psycopg2
import mysql.connector

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define file paths for each country's data
data_paths = {
    "usa": "final_data/country_usa.csv",
    "uk": "final_data/country_uk.csv",
    "india": "final_data/country_india.csv"
}

# Create a MySQL table if it does not exist
def create_mysql_table(user, password, host, db, table_name):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=db
        )
        cur = conn.cursor()
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            name VARCHAR(100),
            gender VARCHAR(10),
            country VARCHAR(20),
            department VARCHAR(50),
            designation VARCHAR(100),
            email VARCHAR(100),
            signup_date DATE,
            address TEXT
        );
        """
        cur.execute(create_query)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"MySQL table creation failed: {e}")

# Create a PostgreSQL table if it does not exist
def create_postgres_table(user, password, host, db, table_name):
    try:
        conn = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=password
        )
        cur = conn.cursor()
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            name VARCHAR(100),
            gender VARCHAR(10),
            country VARCHAR(20),
            department VARCHAR(50),
            designation VARCHAR(100),
            email VARCHAR(100),
            signup_date DATE,
            address TEXT
        );
        """
        cur.execute(create_query)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"PostgreSQL table creation failed: {e}")

# Load a DataFrame into a MySQL table
def load_to_mysql(df, table_name, user, password, host, db):
    try:
        engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Data loaded into MySQL table: {table_name}")
    except Exception as e:
        logging.error(f"MySQL data load failed: {e}")

# Load a DataFrame into a PostgreSQL table
def load_to_postgres(df, table_name, user, password, host, db):
    try:
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{db}")
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Data loaded into PostgreSQL table: {table_name}")
    except Exception as e:
        logging.error(f"PostgreSQL data load failed: {e}")

# Generate metadata about the datasets and save it as a JSON file
def generate_metadata(dataframes, output_dir="final_data"):
    metadata = {
        "timestamp": datetime.now().isoformat(),
        "total_records": sum(len(df) for df in dataframes.values()),
        "records_per_country": {k: len(v) for k, v in dataframes.items()},
        "columns": {
            col: {
                "dtype": str(df[col].dtype),
                "sample_value": df[col].iloc[0] if not df.empty else None
            }
            for df in dataframes.values()
            for col in df.columns
        }
    }

    os.makedirs(output_dir, exist_ok=True)
    metadata_path = os.path.join(output_dir, "metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=4)

# Main workflow
def main():
    # Load CSV files into DataFrames
    dfs = {country: pd.read_csv(path) for country, path in data_paths.items()}

    # Process and store USA data into MySQL
    create_mysql_table("root", "Mag12345!", "localhost", "customer_us_db", "customers_usa")
    load_to_mysql(dfs["usa"], "customers_usa", "root", "Mag12345!", "localhost", "customer_us_db")

    # Combine UK and India data and store into PostgreSQL
    combined_df = pd.concat([dfs["uk"], dfs["india"]], ignore_index=True)
    create_postgres_table("postgres", "postgres", "localhost", "customer_global_db", "customers_global")
    load_to_postgres(combined_df, "customers_global", "postgres", "postgres", "localhost", "customer_global_db")

    # Generate and save metadata
    generate_metadata(dfs)

if __name__ == "__main__":
    main()
