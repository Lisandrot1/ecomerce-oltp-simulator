from utils.logging import logs
from faker import Faker
from sqlalchemy import text
import random
import json
from pathlib import Path
from datetime import datetime, timedelta




faker = Faker() # Usamos genérico para que no se sesgue solo a es_CO en ciudades
log = logs()

# Apunta al archivo donde está este código (ecommerce.py)
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'

def load_geo_metadata():
    with open(DATA_DIR / 'metadata_geo.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def insert_users(conn, volume=5):
    try:
        geo_data = load_geo_metadata()
        email_domains = ['gmail.com', 'outlook.com', 'yahoo.com', 'icloud.com', 'protonmail.com', 'zoho.com', 'hotmail.com']
        
        # Obtener emails y teléfonos existentes para evitar duplicados
        result = conn.execute(text("SELECT email, phone FROM USERS"))
        rows = result.fetchall()
        existing_emails = {row[0] for row in rows}
        existing_phones = {row[1] for row in rows}
        
        user_ids = []
        for _ in range(volume):
            first_name = faker.first_name()
            last_name = faker.last_name()
            email_base = f"{first_name}.{last_name}".lower().replace(' ', '')
            
            # Asegurar email único
            email = f"{email_base}@{random.choice(email_domains)}"
            while email in existing_emails:
                email = f"{email_base}{random.randint(1, 9999)}@{random.choice(email_domains)}"
            
            # Asegurar teléfono único
            phone = f"+{random.randint(1, 99)} {random.randint(100, 999)} {random.randint(1000, 9999)}"
            while phone in existing_phones:
                phone = f"+{random.randint(1, 99)} {random.randint(100, 999)} {random.randint(1000, 9999)}"

            geo = random.choice(geo_data)
            user_data = {
                'name_user': first_name,
                'lastname': last_name,
                'address': faker.address().replace('\n', ', '),
                'email': email,   
                'phone': phone,
                'city': random.choice(geo['cities']),
                'country': geo['country']
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
            existing_emails.add(email)
            existing_phones.add(phone)

        conn.commit()
        log.info(f'Insert USERS Successfully — {len(user_ids)} registros')
        return user_ids
    except Exception as ex:
        conn.rollback()
        log.error(f'ERROR en insert_users: {ex}', exc_info=True)
        return []

def insert_categories(conn):
    try:
        log.info('Iniciando creacion/actualizacion de CATEGORIES')
        with open(DATA_DIR / 'categories.json', 'r', encoding='utf-8') as f:
            categories_list = json.load(f)

        # Obtener existentes
        result = conn.execute(text("SELECT name_category, category_id FROM categories"))
        category_map = {row[0]: row[1] for row in result.fetchall()}

        for cat in categories_list:
            if cat['name'] not in category_map:
                result = conn.execute(
                    text("""
                        INSERT INTO categories (name_category, description)
                        VALUES (:name, :description)
                        RETURNING category_id
                    """),
                    cat
                )
                category_map[cat['name']] = result.fetchone()[0]
            
        conn.commit()
        log.info(f'CATEGORIES listas — {len(category_map)} registros en total')
        return category_map
    except Exception as ex:
        conn.rollback()
        log.error(f'ERROR en insert_categories: {ex}', exc_info=True)
        return {}

def insert_providers(conn):
    try:
        log.info('Iniciando creacion/actualizacion de PROVIDERS')
        with open(DATA_DIR / 'providers.json', 'r', encoding='utf-8') as f:
             providers_list = json.load(f)
             
        # Obtener existentes
        result = conn.execute(text("SELECT name_provider, provider_id FROM providers"))
        provider_map = {row[0]: row[1] for row in result.fetchall()}

        for prov in providers_list:
            if prov['name'] not in provider_map:
                result = conn.execute(
                    text("""
                        INSERT INTO providers (name_provider, address, email, status)
                        VALUES (:name, :address, :email, 'active')
                        RETURNING provider_id
                    """),
                    prov
                )
                provider_map[prov['name']] = result.fetchone()[0]
            
        conn.commit()
        log.info(f'PROVIDERS listos — {len(provider_map)} registros en total')
        return provider_map
    except Exception as ex:
        conn.rollback()
        log.error(f'ERROR en insert_providers: {ex}', exc_info=True)
        return {}


def insert_products(conn, category_ids: dict, provider_ids: dict):
    try:
        log.info('Iniciando creacion/actualizacion de PRODUCTS')
        with open(DATA_DIR / 'products.json', 'r', encoding='utf-8') as f:
            products_list = json.load(f)
            
        # Obtener existentes para evitar duplicados por código
        result = conn.execute(text("SELECT code, products_id, sales_price FROM products"))
        product_price_map = {row[0]: (row[1], float(row[2])) for row in result.fetchall()}
        
        final_price_map = {}
        for p in products_list:
            code = p.get('code')
            if code in product_price_map:
                p_id, price = product_price_map[code]
                final_price_map[p_id] = price
                continue

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
            final_price_map[row[0]] = float(row[1])
            
        conn.commit()
        log.info(f'PRODUCTS listos — {len(final_price_map)} registros en total')
        return final_price_map
    except Exception as ex:
        conn.rollback()
        log.error(f'ERROR en insert_products: {ex}', exc_info=True)
        return {}


def get_all_user_ids(conn):
    """Retorna todos los IDs de usuarios existentes en la DB."""
    try:
        result = conn.execute(text("SELECT user_id FROM USERS"))
        return [row[0] for row in result.fetchall()]
    except Exception as ex:
        log.error(f'ERROR en get_all_user_ids: {ex}')
        return []


def get_product_price_map(conn):
    """Retorna un mapeo de products_id -> sales_price para productos existentes."""
    try:
        result = conn.execute(text("SELECT products_id, sales_price FROM PRODUCTS WHERE status = 'active'"))
        return {row[0]: float(row[1]) for row in result.fetchall()}
    except Exception as ex:
        log.error(f'ERROR en get_product_price_map: {ex}')
        return {}


def insert_orders(conn, user_ids, volume=100):
    try:
        log.info(f'Iniciando creacion de {volume} ORDERS')
        order_ids = []
        statuses = ['pending', 'processing', 'completed', 'cancelled']
        
        if not user_ids:
            log.error("No hay usuarios disponibles para crear ordenes.")
            return []

        for _ in range(volume):
            user_id = random.choice(user_ids)
            
            order_data = {
                'user_id': user_id,
                'shipping_cost': round(random.uniform(5.0, 20.0), 2),
                'total_amount': 0.0,
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