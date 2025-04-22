import os
from datetime import datetime
import pandas as pd

def clean_customer_data(input_file, output_dir="final_data"):
    # Load the Excel file
    df = pd.read_excel(input_file)

    # Drop rows with nulls and duplicates
    df_cleaned = df.dropna().drop_duplicates()

    # Standardize signup_date to YYYY-MM-DD format
    df_cleaned['signup_date'] = pd.to_datetime(df_cleaned['signup_date']).dt.strftime('%Y-%m-%d')

    # Strip whitespace from all string fields
    str_columns = ['name', 'gender', 'country', 'department', 'designation', 'email', 'address']
    for col in str_columns:
        df_cleaned[col] = df_cleaned[col].astype(str).str.strip()

    # Convert relevant fields to lowercase
    for col in ['email', 'address', 'designation', 'country']:
        df_cleaned[col] = df_cleaned[col].str.lower()

    # Normalize country values to only usa, uk, india
    df_cleaned = df_cleaned[df_cleaned['country'].isin(['usa', 'uk', 'india'])]

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Split and save cleaned data by country
    for country in ['usa', 'uk', 'india']:
        country_df = df_cleaned[df_cleaned['country'] == country]
        country_df.to_csv(os.path.join(output_dir, f"country_{country}.csv"), index=False)

if __name__ == "__main__":
    clean_customer_data("messy_customers_data.xlsx")
