from sqlalchemy import create_engine, text
import os




def get_connection():
    
    # credenciales desde .env
    HOST = os.environ['NEON_DB_HOST']
    USER = os.environ['NEON_POSTGRES_USER']
    PASSWORD = os.environ['NEON_POSTGRES_PASSWORD']
    DATABASE = os.environ['NEON_DATABASE']
    PORT = os.environ['NEON_DB_PORT']

    engine = create_engine(
        f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?sslmode=require',
        echo=False
        )

    return engine