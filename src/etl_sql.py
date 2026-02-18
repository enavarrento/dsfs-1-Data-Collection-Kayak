import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. Load Config
load_dotenv()

USER = os.getenv("AWS_RDS_USER")
PASSWORD = os.getenv("AWS_RDS_PASSWORD")
HOST = os.getenv("AWS_RDS_HOST")
PORT = os.getenv("AWS_RDS_PORT")
DB_NAME = os.getenv("AWS_RDS_DB_NAME")

def load_to_sql():
    print("🚀 Starting ETL Pipeline...")
    
    # 2. Extract (Read the clean data)
    # Ideally, we read from S3, but reading the local identical file is 
    # perfectly standard for this step to save bandwidth/complexity.
    csv_path = "data/processed/kayak_master.csv"
    if not os.path.exists(csv_path):
        print("❌ Error: Processed CSV not found.")
        return

    df = pd.read_csv(csv_path)
    print(f"📦 Data extracted. Shape: {df.shape}")

    # 3. Connect to SQL Database
    # Connection String format: postgresql+psycopg2://user:password@host:port/dbname
    conn_string = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
    
    try:
        engine = create_engine(conn_string)
        
        # Test connection
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("🔌 Connection to RDS successful!")

        # 4. Load (Write to SQL)
        # 'replace' drops the table if it exists and creates a new one.
        table_name = "destinations"
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(f"🎉 Success! Data loaded into table '{table_name}'.")
        
        # Verify
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            print(f"📊 Verification: {count} rows found in SQL.")

    except Exception as e:
        print(f"❌ Database Error: {e}")

if __name__ == "__main__":
    load_to_sql()