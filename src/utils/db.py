from sqlalchemy import create_engine, text
import os


# credenciales desde .env
HOST = os.environ['HOST']
USER = os.environ['POSTGRES_USER']
PASSWORD = os.environ['POSTGRES_PASSWORD']
DATABASE = os.environ['DATABASE']

def get_connection():
    engine = create_engine(
        f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:5432/{DATABASE}',
        echo=False
        )

    return engine