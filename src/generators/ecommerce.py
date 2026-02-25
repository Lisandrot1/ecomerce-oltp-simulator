from utils.logging import logs
from faker import Faker
from sqlalchemy import text
import random
import json
from pathlib import Path
from datetime import datetime, timedelta




faker = Faker('es_CO')
log = logs()

def insert_users(conn, volume=2):
    try:
        user_ids = []
        for _ in range(volume):
            # Sacamos los nombres
            first_name = faker.first_name()
            last_name = faker.last_name()
            # aca concatenamos el nombre con el apellido
            email_name = f"{first_name}.{last_name}".lower()
            #aca se une el nombre con el dominio del email
            email = f"{email_name}@{faker.free_email_domain()}"
            
            user_data = {
                'name_user': first_name,
                'lastname': last_name,
                'address': faker.address().replace('\n', ', '),
                'email': email,   
                'phone': faker.phone_number().replace(' ', ''),
                'city': faker.city(),
                'country': 'Colombia'
            }

            result = conn.execute(
                text("""
                    INSERT INTO USERS (name_user, lastname, address, email, phone, city, country) 
                    VALUES (:name_user, :lastname, :address, :email, :phone, :city, :country)
                    RETURNING user_id
                """),
                user_data
            )
            user_ids.append(result.fetchone()[0])

        conn.commit()
        log.info(f'Insert USERS Successfully — {len(user_ids)} registros')
        return user_ids
        
    except Exception as ex:
        log.error(f'ERROR en insert_users: {ex}', exc_info=True)
        return []

# Apunta al archivo donde está este código (ecommerce.py)
BASE_DIR = Path(__file__).parent.parent  # sube a src/
# o si los JSON están en la misma carpeta que ecommerce.py:
BASE_DIR = Path(__file__).parent


def insert_categories(conn):
    try:
        log.info('Iniciando creacion de CATEGORIES')

        with open(BASE_DIR / 'data' / 'categories.json', 'r', encoding='utf-8') as f:
            categories_list = json.load(f)

        conn.execute(text("TRUNCATE TABLE categories RESTART IDENTITY CASCADE;"))

        category_map = {}
        for cat in categories_list:
            result = conn.execute(
                text("""
                    INSERT INTO categories (name_category, description)
                    VALUES (:name, :description)
                    RETURNING category_id, name_category
                """),
                cat
            )
            row = result.fetchone()
            category_map[row[1]] = row[0]
            
        conn.commit()
        log.info(f'Insert CATEGORIES exitoso — {len(category_map)} registros')
        return category_map

    except Exception as ex:
        conn.rollback()
        log.error(f'ERROR en insert_categories: {ex}', exc_info=True)
        return {}


def insert_providers(conn):
    try:
        log.info('Iniciando creacion de PROVIDERS')

        # Corregido el nombre del archivo
        with open(BASE_DIR / 'data' / 'providers.json', 'r', encoding='utf-8') as f:
             providers_list = json.load(f)
             
        conn.execute(text("TRUNCATE TABLE providers RESTART IDENTITY CASCADE;"))

        provider_map = {}
        for prov in providers_list:
            result = conn.execute(
                text("""
                    INSERT INTO providers (name_provider, address, email, status)
                    VALUES (:name, :address, :email, 'active')
                    RETURNING provider_id, name_provider
                """),
                prov
            )
            row = result.fetchone()
            provider_map[row[1]] = row[0]
            
        conn.commit()
        log.info(f'Insert PROVIDERS exitoso — {len(provider_map)} registros')
        return provider_map

    except Exception as ex:
        conn.rollback()
        log.error(f'ERROR en insert_providers: {ex}', exc_info=True)
        return {}


def insert_products(conn, category_ids: dict, provider_ids: dict):
    try:
        log.info('Iniciando creacion de PRODUCTS')

        with open(BASE_DIR / 'data' / 'products.json', 'r', encoding='utf-8') as f:
            products_list = json.load(f)
            
        conn.execute(text("TRUNCATE TABLE products RESTART IDENTITY CASCADE;"))

        product_price_map = {}
        for p in products_list:
            # Mapeamos los nombres a sus respectivos IDs
            cat_name = p.get('category')
            prov_name = p.get('provider')
            p['category_id'] = category_ids.get(cat_name)
            p['provider_id'] = provider_ids.get(prov_name)
            
            if p['category_id'] is None or p['provider_id'] is None:
                log.warning(f"Skipping product '{p['name']}': Category '{cat_name}' or Provider '{prov_name}' not found.")
                continue

            result = conn.execute(
                text("""
                    INSERT INTO products 
                        (name_product, category_id, provider_id, code,
                         cost_price, sales_price, stock, status)
                    VALUES 
                        (:name, :category_id, :provider_id, :code,
                         :cost_price, :sales_price, :stock, 'active')
                    RETURNING products_id, sales_price
                """),
                p
            )
            row = result.fetchone()
            product_price_map[row[0]] = float(row[1])
            
        conn.commit()
        log.info(f'Insert PRODUCTS exitoso — {len(product_price_map)} registros')
        return product_price_map

    except Exception as ex:
        conn.rollback()
        log.error(f'ERROR en insert_products: {ex}', exc_info=True)
        return {}


def insert_orders(conn, user_ids, volume_per_user=1):
    try:
        log.info('Iniciando creacion de ORDERS')
        order_ids = []
        statuses = ['pending', 'processing', 'completed', 'cancelled']
        
        for user_id in user_ids:
            for _ in range(volume_per_user):
                order_data = {
                    'user_id': user_id,
                    'shipping_cost': round(random.uniform(5.0, 20.0), 2),
                    'total_amount': 0.0, # Se actualizará después o se puede dejar en 0 por ahora
                    'status': random.choice(statuses)
                }
                
                result = conn.execute(
                    text("""
                        INSERT INTO ORDERS (user_id, shipping_cost, total_amount, status)
                        VALUES (:user_id, :shipping_cost, :total_amount, :status)
                        RETURNING orders_id
                    """),
                    order_data
                )
                order_ids.append(result.fetchone()[0])
        
        conn.commit()
        log.info(f'Insert ORDERS exitoso — {len(order_ids)} registros')
        return order_ids
    except Exception as ex:
        conn.rollback()
        log.error(f'ERROR en insert_orders: {ex}', exc_info=True)
        return []


def insert_order_details(conn, order_ids, product_price_map):
    try:
        log.info('Iniciando creacion de ORDERS_DETAILS')
        product_ids = list(product_price_map.keys())
        
        if not product_ids:
            log.error("No hay productos disponibles para crear detalles de orden.")
            return

        for order_id in order_ids:
            num_items = random.randint(1, 5)
            order_total = 0.0
            
            # Seleccionar productos aleatorios para esta orden
            selected_products = random.sample(product_ids, min(num_items, len(product_ids)))
            
            for p_id in selected_products:
                qty = random.randint(1, 10)
                price = product_price_map[p_id]
                subtotal = qty * price
                order_total += subtotal
                
                detail_data = {
                    'products_id': p_id,
                    'order_id': order_id,
                    'quantity': qty,
                    'unit_price': price
                }
                
                conn.execute(
                    text("""
                        INSERT INTO ORDERS_DETAILS (products_id, order_id, quantity, unit_price)
                        VALUES (:products_id, :order_id, :quantity, :unit_price)
                    """),
                    detail_data
                )
            
            # Actualizar el total de la orden
            conn.execute(
                text("UPDATE ORDERS SET total_amount = :total_amount WHERE orders_id = :order_id"),
                {'total_amount': order_total, 'order_id': order_id}
            )
            
        conn.commit()
        log.info(f'Insert ORDERS_DETAILS y actualización de totales exitosa')
    except Exception as ex:
        conn.rollback()
        log.error(f'ERROR en insert_order_details: {ex}', exc_info=True)


def insert_payments(conn, order_ids):
    try:
        log.info('Iniciando creacion de PAYMENTS')
        methods = ['Credit Card', 'WhatsApp', 'PSE', 'Cash']
        
        for order_id in order_ids:
            # Consultar el total y el estado de la orden para el pago
            result = conn.execute(
                text("SELECT total_amount, status FROM ORDERS WHERE orders_id = :order_id"),
                {'order_id': order_id}
            )
            row = result.fetchone()
            if not row: continue
            
            amount, order_status = row
            
            payment_status = 'paid' if order_status == 'completed' else 'pending'
            if order_status == 'cancelled': payment_status = 'refunded'
            
            payment_data = {
                'order_id': order_id,
                'payment_method': random.choice(methods),
                'amount': amount,
                'status': payment_status
            }
            
            conn.execute(
                text("""
                    INSERT INTO PAYMENTS (order_id, payment_method, amount, status)
                    VALUES (:order_id, :payment_method, :amount, :status)
                """),
                payment_data
            )
            
        conn.commit()
        log.info(f'Insert PAYMENTS exitoso')
    except Exception as ex:
        conn.rollback()
        log.error(f'ERROR en insert_payments: {ex}', exc_info=True)