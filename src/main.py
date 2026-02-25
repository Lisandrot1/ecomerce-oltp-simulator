from utils.logging import logs
from utils.db import get_connection
from generators.ecommerce import (
    insert_categories,
    insert_providers,
    insert_products,
    insert_users,
    insert_orders,
    insert_order_details,
    insert_payments
)

log = logs(__name__)

def main_ecommerce():
    try:
        with get_connection().connect() as conn:
            log.info('=' * 50)
            log.info('Iniciando Generador de E-commerce')

            # Sin dependencias — retornan dict {nombre: id}
            category_ids = insert_categories(conn)
            provider_ids = insert_providers(conn)

            # Necesita los dicts para mapear nombre → id
            # Ahora insert_products retorna id: price
            product_price_map = insert_products(conn, category_ids, provider_ids)

            # Solo necesita IDs simples
            user_ids = insert_users(conn)

            # --- NUEVO: Flujo de órdenes ---
            if user_ids and product_price_map:
                # Generamos órdenes
                order_ids = insert_orders(conn, user_ids, volume_per_user=2)
                
                # Generamos detalles (esto actualizará el total de la orden)
                insert_order_details(conn, order_ids, product_price_map)
                
                # Generamos pagos basándonos en las órdenes y sus totales
                insert_payments(conn, order_ids)

            log.info('Generador de E-commerce finalizado')
            log.info('=' * 50)

    except Exception as ex:
        log.error(f'ERROR en MAIN: {ex}', exc_info=True)


if __name__ == "__main__":
    main_ecommerce()