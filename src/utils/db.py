from sqlalchemy import create_engine
import os

def get_connection(db_type='ecommerce'):
    """
    Returns a SQLAlchemy engine for the specified database type.
    db_type: 'ecommerce', 'rrhh', or 'marketing'
    """
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    
    if db_type == 'ecommerce':
        host = os.getenv('ECOMMERCE_DB_HOST', 'localhost')
        port = os.getenv('ECOMMERCE_DB_PORT', '5432')
        name = os.getenv('ECOMMERCE_DB_NAME', 'db_ecommerce')
    elif db_type == 'rrhh':
        host = os.getenv('RRHH_DB_HOST', 'localhost')
        port = os.getenv('RRHH_DB_PORT', '5433')
        name = os.getenv('RRHH_DB_NAME', 'db_rrhh')
    elif db_type == 'marketing':
        host = os.getenv('MARKETING_DB_HOST', 'localhost')
        port = os.getenv('MARKETING_DB_PORT', '5434')
        name = os.getenv('MARKETING_DB_NAME', 'db_marketing')
    else:
        raise ValueError(f"Invalid db_type: {db_type}")

    url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
    
    engine = create_engine(
        url,
        echo=False
    )

    return engine