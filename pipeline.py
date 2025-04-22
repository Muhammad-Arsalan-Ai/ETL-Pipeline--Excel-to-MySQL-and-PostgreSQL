# # import pandas as pd
# # from sqlalchemy import create_engine
# # from datetime import datetime
# # import json
# # import os
# # import logging

# # # Logging setup
# # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # # Load cleaned CSVs
# # data_paths = {
# #     "usa": "final_data/country_usa.csv",
# #     "uk": "final_data/country_uk.csv",
# #     "india": "final_data/country_india.csv"
# # }

# # def load_to_mysql(df, table_name, user, password, host, db):
# #     try:
# #         engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")
# #         df.to_sql(table_name, con=engine, if_exists='replace', index=False)
# #         logging.info(f"✅ Loaded data into MySQL table `{table_name}`")
# #     except Exception as e:
# #         logging.error(f"MySQL load failed: {e}")

# # def load_to_postgres(df, table_name, user, password, host, db):
# #     try:
# #         engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{db}")
# #         df.to_sql(table_name, con=engine, if_exists='replace', index=False)
# #         logging.info(f"✅ Loaded data into PostgreSQL table `{table_name}`")
# #     except Exception as e:
# #         logging.error(f"PostgreSQL load failed: {e}")

# # def generate_metadata(dataframes):
# #     metadata = {
# #         "timestamp": datetime.now().isoformat(),
# #         "total_records": sum(len(df) for df in dataframes.values()),
# #         "records_per_country": {k: len(v) for k, v in dataframes.items()},
# #         "columns": {
# #             col: {
# #                 "dtype": str(df[col].dtype),
# #                 "sample_value": df[col].iloc[0] if not df.empty else None
# #             }
# #             for df in dataframes.values()
# #             for col in df.columns
# #         }
# #     }
# #     with open("metadata.json", "w") as f:
# #         json.dump(metadata, f, indent=4)
# #     logging.info("📄 Metadata saved to metadata.json")

# # def main():
# #     # Load all country data
# #     dfs = {country: pd.read_csv(path) for country, path in data_paths.items()}

# #     # Load USA data into MySQL
# #     load_to_mysql(
# #         df=dfs["usa"],
# #         table_name="customers_usa",
# #         user="your_mysql_user",
# #         password="your_mysql_password",
# #         host="localhost",
# #         db="customer_us_db"
# #     )

# #     # Load UK and India data into PostgreSQL
# #     combined_df = pd.concat([dfs["uk"], dfs["india"]], ignore_index=True)
# #     load_to_postgres(
# #         df=combined_df,
# #         table_name="customers_global",
# #         user="postgres",
# #         password="postgres",
# #         host="localhost",
# #         db="customer_global_db"
# #     )

# #     # Generate metadata
# #     generate_metadata(dfs)

# # if __name__ == "__main__":
# #     main()




# import pandas as pd
# from sqlalchemy import create_engine
# from datetime import datetime
# import json
# import os
# import logging
# import psycopg2

# # Logging setup
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # Load cleaned CSVs
# data_paths = {
#     "usa": "final_data/country_usa.csv",
#     "uk": "final_data/country_uk.csv",
#     "india": "final_data/country_india.csv"
# }

# def create_postgres_table(user, password, host, db, table_name):
#     try:
#         conn = psycopg2.connect(
#             host=host,
#             database=db,
#             user=user,
#             password=password
#         )
#         cur = conn.cursor()

#         create_table_query = f"""
#         CREATE TABLE IF NOT EXISTS {table_name} (
#             name VARCHAR(100),
#             gender VARCHAR(10),
#             country VARCHAR(20),
#             department VARCHAR(50),
#             designation VARCHAR(100),
#             email VARCHAR(100),
#             signup_date DATE,
#             address TEXT
#         );
#         """

#         cur.execute(create_table_query)
#         conn.commit()
#         logging.info(f"PostgreSQL table `{table_name}` created (if it didn't exist).")

#         cur.close()
#         conn.close()
#     except Exception as e:
#         logging.error(f"PostgreSQL table creation failed: {e}")

# def load_to_mysql(df, table_name, user, password, host, db):
#     try:
#         engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")
#         df.to_sql(table_name, con=engine, if_exists='replace', index=False)
#         logging.info(f" Loaded data into MySQL table `{table_name}`")
#     except Exception as e:
#         logging.error(f"MySQL load failed: {e}")

# def load_to_postgres(df, table_name, user, password, host, db):
#     try:
#         engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{db}")
#         df.to_sql(table_name, con=engine, if_exists='replace', index=False)
#         logging.info(f"Loaded data into PostgreSQL table `{table_name}`")
#     except Exception as e:
#         logging.error(f"PostgreSQL load failed: {e}")

# def generate_metadata(dataframes):
#     metadata = {
#         "timestamp": datetime.now().isoformat(),
#         "total_records": sum(len(df) for df in dataframes.values()),
#         "records_per_country": {k: len(v) for k, v in dataframes.items()},
#         "columns": {
#             col: {
#                 "dtype": str(df[col].dtype),
#                 "sample_value": df[col].iloc[0] if not df.empty else None
#             }
#             for df in dataframes.values()
#             for col in df.columns
#         }
#     }
#     with open("metadata.json", "w") as f:
#         json.dump(metadata, f, indent=4)
#     logging.info("Metadata saved to metadata.json")

# def main():
#     # Load all country data
#     dfs = {country: pd.read_csv(path) for country, path in data_paths.items()}

#     # Load USA data into MySQL
#     load_to_mysql(
#         df=dfs["usa"],
#         table_name="customers_usa",
#         user="your_mysql_user",
#         password="your_mysql_password",
#         host="localhost",
#         db="customer_us_db"
#     )

#     # Combine UK and India data
#     combined_df = pd.concat([dfs["uk"], dfs["india"]], ignore_index=True)

#     # Ensure PostgreSQL table exists before loading
#     create_postgres_table(
#         user="postgres",
#         password="postgres",
#         host="localhost",
#         db="customer_global_db",
#         table_name="customers_global"
#     )

#     # Load UK and India data into PostgreSQL
#     load_to_postgres(
#         df=combined_df,
#         table_name="customers_global",
#         user="postgres",
#         password="postgres",
#         host="localhost",
#         db="customer_global_db"
#     )

#     # Generate metadata
#     generate_metadata(dfs)

# if __name__ == "__main__":
#     main()



# #save metadate save dir
# #mysql create tables ,insert






import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import json
import os
import logging
import psycopg2
import mysql.connector

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load cleaned CSVs
data_paths = {
    "usa": "final_data/country_usa.csv",
    "uk": "final_data/country_uk.csv",
    "india": "final_data/country_india.csv"
}

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
        logging.info(f"🛠️ MySQL table `{table_name}` created (if not exists).")

        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"MySQL table creation failed: {e}")

def create_postgres_table(user, password, host, db, table_name):
    try:
        conn = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=password
        )
        cur = conn.cursor()

        create_table_query = f"""
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

        cur.execute(create_table_query)
        conn.commit()
        logging.info(f"🛠️ PostgreSQL table `{table_name}` created (if it didn't exist).")

        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"PostgreSQL table creation failed: {e}")

def load_to_mysql(df, table_name, user, password, host, db):
    try:
        engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"✅ Loaded data into MySQL table `{table_name}`")
    except Exception as e:
        logging.error(f"MySQL load failed: {e}")

def load_to_postgres(df, table_name, user, password, host, db):
    try:
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{db}")
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"✅ Loaded data into PostgreSQL table `{table_name}`")
    except Exception as e:
        logging.error(f"PostgreSQL load failed: {e}")

# def generate_metadata(dataframes):
#     metadata = {
#         "timestamp": datetime.now().isoformat(),
#         "total_records": sum(len(df) for df in dataframes.values()),
#         "records_per_country": {k: len(v) for k, v in dataframes.items()},
#         "columns": {
#             col: {
#                 "dtype": str(df[col].dtype),
#                 "sample_value": df[col].iloc[0] if not df.empty else None
#             }
#             for df in dataframes.values()
#             for col in df.columns
#         }
#     }
#     with open("metadata.json", "w") as f:
#         json.dump(metadata, f, indent=4)
#     logging.info("📄 Metadata saved to metadata.json")


def generate_metadata(dataframes):
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

    # Ensure the output directory exists
    output_dir = "final_data"
    os.makedirs(output_dir, exist_ok=True)

    metadata_path = os.path.join(output_dir, "metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=4)

    logging.info(f"📄 Metadata saved to {metadata_path}")

def main():
    # Load all country data
    dfs = {country: pd.read_csv(path) for country, path in data_paths.items()}

    # Create MySQL table before inserting USA data
    create_mysql_table(
        user="root",
        password="Mag12345!",
        host="localhost",
        db="customer_us_db",
        table_name="customers_usa"
    )

    # Insert into MySQL
    load_to_mysql(
        df=dfs["usa"],
        table_name="customers_usa",
        user="your_mysql_user",
        password="your_mysql_password",
        host="localhost",
        db="customer_us_db"
    )

    # Combine UK and India
    combined_df = pd.concat([dfs["uk"], dfs["india"]], ignore_index=True)

    # Create PostgreSQL table before inserting
    create_postgres_table(
        user="postgres",
        password="postgres",
        host="localhost",
        db="customer_global_db",
        table_name="customers_global"
    )

    # Insert into PostgreSQL
    load_to_postgres(
        df=combined_df,
        table_name="customers_global",
        user="postgres",
        password="postgres",
        host="localhost",
        db="customer_global_db"
    )

    # Generate metadata
    generate_metadata(dfs)

if __name__ == "__main__":
    main()
