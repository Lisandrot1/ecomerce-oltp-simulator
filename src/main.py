from utils.logging import logs
from utils.db import get_connection
from generators.ecommerce import (
    insert_categories,
    insert_providers,
    insert_products,
    insert_users,
    insert_orders,
    insert_order_details,
    insert_payments,
    get_all_user_ids,
    get_product_price_map
)
import random

log = logs(__name__)

def main_ecommerce():
    try:
        with get_connection().connect() as conn:
            log.info('=' * 50)
            log.info('Iniciando Simulador Diario de E-commerce')

            # --- 1. METADATA (Solo si no existen) ---
            # Intentamos obtener los productos existentes
            product_price_map = get_product_price_map(conn)
            
            if not product_price_map:
                log.info("Inicializando metadata (Categorías, Proveedores, Productos)...")
                category_ids = insert_categories(conn)
                provider_ids = insert_providers(conn)
                product_price_map = insert_products(conn, category_ids, provider_ids)
            else:
                log.info("Usando metadata existente.")

            # --- 2. USUARIOS NUEVOS (15 - 20 por día) ---
            new_users_volume = random.randint(15, 20)
            log.info(f"Simulando {new_users_volume} nuevos usuarios...")
            insert_users(conn, volume=new_users_volume)

            # --- 3. ÓRDENES (1,000 - 2,000 por día) ---
            # Obtenemos todos los IDs de usuarios (viejos + nuevos)
            all_user_ids = get_all_user_ids(conn)
            
            if all_user_ids and product_price_map:
                daily_orders_volume = random.randint(1000, 2000)
                log.info(f"Simulando {daily_orders_volume} nuevas órdenes...")
                
                # Generamos órdenes
                order_ids = insert_orders(conn, all_user_ids, volume=daily_orders_volume)
                
                # Generamos detalles
                insert_order_details(conn, order_ids, product_price_map)
                
                # Generamos pagos
                insert_payments(conn, order_ids)
            else:
                log.warning("No hay usuarios o productos suficientes para generar órdenes.")

            log.info('Simulador diario finalizado')
            log.info('=' * 50)

    except Exception as ex:
        log.error(f'ERROR en MAIN: {ex}', exc_info=True)


if __name__ == "__main__":
    main_ecommerce()