# ETL Pipeline: Excel to MySQL and PostgreSQL

## Project Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline using Python. It processes raw customer data from an Excel file, cleans and organizes it, stores country-specific data in relational databases (MySQL and PostgreSQL), and generates structured metadata.

## Features

- Read customer data from an Excel file
- Clean and transform the data:
  - Remove null and duplicate records
  - Standardize date format to YYYY-MM-DD
  - Normalize country names to: usa, uk, india
  - Lowercase relevant text fields
  - Strip whitespace
- Save cleaned data as separate CSV files by country
- Load data into relational databases:
  - USA data into a MySQL database
  - UK and India data into a PostgreSQL database
- Generate metadata and export it as a JSON file

## Directory Structure

```
.
├── data_cleaning.py           # Extracts and cleans raw Excel data
├── etl_pipeline.py            # Loads cleaned data into MySQL/PostgreSQL and generates metadata
├── requirements.txt           # Python dependencies
├── README.md                  # Project documentation
└── final_data/                # Output directory containing:
    ├── country_usa.csv
    ├── country_uk.csv
    ├── country_india.csv
    └── metadata.json
```

## Setup Instructions

1. Install Python 3.x
2. Clone this repository
3. Create and activate a virtual environment using `virtualenv`:

```bash
python -m pip install virtualenv
virtualenv env
# Activate the environment:
# For Windows:
env\Scripts\activate
# For macOS/Linux:
source env/bin/activate
```

4. Install required Python packages:

```bash
pip install -r requirements.txt
```

## Database Setup

### In MySQL Workbench:

```sql
CREATE DATABASE customer_us_db;
```

### In PostgreSQL (via pgAdmin or CLI):

```sql
CREATE DATABASE customer_global_db;
```

## How to Run

### Step 1: Clean the Excel Data

```bash
python data_cleaning.py
```

This will generate the following cleaned CSV files inside the `final_data/` directory:

- `country_usa.csv`
- `country_uk.csv`
- `country_india.csv`

### Step 2: Load Data and Generate Metadata

```bash
python etl_pipeline.py
```

This will:
- Create tables if they do not exist
- Load data into:
  - `customers_usa` table in MySQL (`customer_us_db`)
  - `customers_global` table in PostgreSQL (`customer_global_db`)
- Generate `metadata.json` inside the `final_data/` directory

## Output

- Cleaned CSV files: `final_data/country_*.csv`
- Metadata file: `final_data/metadata.json`
- MySQL Table: `customers_usa`
- PostgreSQL Table: `customers_global`

## Technologies Used

- Python
- Pandas
- SQLAlchemy
- MySQL
- PostgreSQL
- JSON