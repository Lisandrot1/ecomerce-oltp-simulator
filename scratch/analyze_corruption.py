import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def analyze_nulls(db_name, port, table_name):
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', 'postgresql')
    host = 'localhost'
    
    url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(url)
    
    print(f"\n--- Analyzing table {table_name} in {db_name} ---")
    try:
        with engine.connect() as conn:
            # Total rows
            total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            if total_rows == 0:
                print("Table is empty.")
                return

            print(f"Total rows: {total_rows}")
            
            # Get columns
            columns_res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name.lower()}'"))
            columns = [row[0] for row in columns_res]
            
            print("\nNull Percentages:")
            for col in columns:
                null_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL")).scalar()
                if null_count > 0:
                    pct = (null_count / total_rows) * 100
                    print(f"{col}: {null_count} ({pct:.2f}%)")
            
            # Duplicate analysis
            # We count rows that appear more than once
            cols_str = ", ".join(columns)
            dup_query = f"""
                SELECT SUM(count - 1) 
                FROM (
                    SELECT COUNT(*) as count 
                    FROM {table_name} 
                    GROUP BY {cols_str} 
                    HAVING COUNT(*) > 1
                ) as dups
            """
            dup_count = conn.execute(text(dup_query)).scalar() or 0
            dup_pct = (dup_count / total_rows) * 100
            print(f"\nDuplicate rows (full match): {dup_count} ({dup_pct:.2f}%)")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_nulls('db_ecommerce', '5432', 'orders')
    analyze_nulls('db_ecommerce', '5432', 'orders_details')
    analyze_nulls('db_ecommerce', '5432', 'payments')
