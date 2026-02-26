from sqlalchemy import create_engine, text
import os




def get_connection():
    
    # credenciales desde .env
    url = os.environ['DATABASE_URL']

    engine = create_engine(
        url,
        echo=False
    )

    return engine