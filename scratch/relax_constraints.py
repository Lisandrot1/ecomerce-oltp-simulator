from sqlalchemy import create_engine, text
import os

def run_migrations():
    user = "postgres"
    password = "postgresql"
    
    dbs = {
        'ecommerce': ('localhost', '5432', 'db_ecommerce'),
        'rrhh': ('localhost', '5433', 'db_rrhh'),
        'marketing': ('localhost', '5434', 'db_marketing')
    }
    
    migrations = {
        'ecommerce': [
            "ALTER TABLE ORDERS ALTER COLUMN shipping_cost DROP NOT NULL",
            "ALTER TABLE ORDERS ALTER COLUMN total_amount DROP NOT NULL",
            "ALTER TABLE ORDERS ALTER COLUMN status DROP NOT NULL",
            "ALTER TABLE PAYMENTS ALTER COLUMN payment_method DROP NOT NULL",
            "ALTER TABLE PAYMENTS ALTER COLUMN amount DROP NOT NULL",
            "ALTER TABLE PAYMENTS ALTER COLUMN status DROP NOT NULL",
            "ALTER TABLE ORDERS_DETAILS ALTER COLUMN quantity DROP NOT NULL",
            "ALTER TABLE ORDERS_DETAILS ALTER COLUMN unit_price DROP NOT NULL"
        ],
        'marketing': [
            "ALTER TABLE LEADS ALTER COLUMN first_name DROP NOT NULL",
            "ALTER TABLE LEADS ALTER COLUMN last_name DROP NOT NULL",
            "ALTER TABLE LEADS ALTER COLUMN email DROP NOT NULL",
            "ALTER TABLE LEADS ALTER COLUMN source DROP NOT NULL",
            "ALTER TABLE LEADS ALTER COLUMN status DROP NOT NULL",
            "ALTER TABLE CUSTOMER_SEGMENT_ASSIGNMENT ALTER COLUMN assigned_date DROP NOT NULL",
            "ALTER TABLE EMAIL_CAMPAIGN_EVENTS ALTER COLUMN event_type DROP NOT NULL"
        ],
        'rrhh': [
            "ALTER TABLE ATTENDANCE ALTER COLUMN status DROP NOT NULL",
            "ALTER TABLE PAYROLL ALTER COLUMN period_start DROP NOT NULL",
            "ALTER TABLE PAYROLL ALTER COLUMN period_end DROP NOT NULL",
            "ALTER TABLE PAYROLL ALTER COLUMN base_salary DROP NOT NULL",
            "ALTER TABLE PAYROLL ALTER COLUMN total_payment DROP NOT NULL",
            "ALTER TABLE PERFORMANCE ALTER COLUMN review_date DROP NOT NULL",
            "ALTER TABLE PERFORMANCE ALTER COLUMN score DROP NOT NULL"
        ]
    }
    
    for db_key, (host, port, name) in dbs.items():
        print(f"Migrating {db_key}...")
        url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
        try:
            engine = create_engine(url)
            with engine.connect() as conn:
                for sql in migrations[db_key]:
                    conn.execute(text(sql))
                conn.commit()
            print(f"Successfully migrated {db_key}.")
        except Exception as e:
            print(f"Failed to migrate {db_key}: {e}")

if __name__ == "__main__":
    run_migrations()
