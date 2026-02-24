from utils.logging import logs
from generators.ecommerce import insert_users
from utils.db import get_connection
log = logs(__name__)

def main_ecommerce():
    try:
        
        with get_connection().connect() as conn:            
            log.info('='*50)
            log.info('Iniciando con Generador de E-commerce')
            insert_users(conn)  
            log.info('Insercion de user Terminado')
    except Exception as ex:
        log.error(f'ERROR en MAIN: {ex}', exc_info=True)
        
        
if __name__ == "__main__":
    try:
       main_ecommerce()
    except Exception as ex:
        log.error(f'ERROR en MAIN: {ex}', exc_info=True)
    