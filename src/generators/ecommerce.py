from utils.logging import logs
from faker import Faker
from sqlalchemy import text
import random
import json
from pathlib import Path
from datetime import datetime, timedelta




faker = Faker() # Usamos genérico para que no se sesgue solo a es_CO en ciudades
log = logs()

def apply_corruption(data, fields=None, prob=0.0, duplicate_prob=0.0):
    """
    Aplica suciedad a los datos: nulos en campos específicos o duplicados.
    Retorna (data_modificada, debe_duplicar)
    """
    should_duplicate = False
    
    # Corrupción de campos (Nulos)
    if fields and random.random() < prob:
        # Seleccionamos al menos uno de los campos especificados para poner en NULL
        to_null = random.sample(fields, random.randint(1, len(fields)))
        for f in to_null:
            if f in data:
                data[f] = None
                
    # Duplicados
    if random.random() < duplicate_prob:
        should_duplicate = True
        
    return data, should_duplicate

def relax_ecommerce_constraints(conn):
    """
    Relaja las restricciones de la DB de Ecommerce para permitir la simulación de datos sucios.
    """
    try:
        # USERS
        conn.execute(text("ALTER TABLE USERS ALTER COLUMN name_user DROP NOT NULL"))
        conn.execute(text("ALTER TABLE USERS ALTER COLUMN lastname DROP NOT NULL"))
        conn.execute(text("ALTER TABLE USERS ALTER COLUMN address DROP NOT NULL"))
        conn.execute(text("ALTER TABLE USERS ALTER COLUMN email DROP NOT NULL"))
        conn.execute(text("ALTER TABLE USERS ALTER COLUMN phone DROP NOT NULL"))
        conn.execute(text("ALTER TABLE USERS ALTER COLUMN city DROP NOT NULL"))
        conn.execute(text("ALTER TABLE USERS ALTER COLUMN country DROP NOT NULL"))
        conn.execute(text("ALTER TABLE USERS DROP CONSTRAINT IF EXISTS users_email_key"))
        conn.execute(text("ALTER TABLE USERS DROP CONSTRAINT IF EXISTS users_phone_key"))
        
        # PRODUCTS
        conn.execute(text("ALTER TABLE PRODUCTS ALTER COLUMN stock DROP NOT NULL"))
        conn.execute(text("ALTER TABLE PRODUCTS ALTER COLUMN status DROP NOT NULL"))
        conn.execute(text("ALTER TABLE PRODUCTS DROP CONSTRAINT IF EXISTS products_code_key"))
        
        # PROVIDERS
        conn.execute(text("ALTER TABLE PROVIDERS ALTER COLUMN status DROP NOT NULL"))
        conn.execute(text("ALTER TABLE PROVIDERS DROP CONSTRAINT IF EXISTS providers_email_key"))
        
        # ORDERS
        conn.execute(text("ALTER TABLE ORDERS ALTER COLUMN status DROP NOT NULL"))
        
        # CATEGORIES
        conn.execute(text("ALTER TABLE CATEGORIES ALTER COLUMN name_category DROP NOT NULL"))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.warning(f"No se pudo relajar las restricciones de Ecommerce: {e}")

# Apunta al archivo donde está este código (ecommerce.py)
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'

def load_geo_metadata():
    with open(DATA_DIR / 'metadata_geo.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def insert_users(conn, volume=5):
    try:
        relax_ecommerce_constraints(conn)
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

            # Aplicar corrupción 5% nulos y 5% duplicados
            user_data, should_duplicate = apply_corruption(user_data, fields=['email', 'phone', 'address'], prob=0.05, duplicate_prob=0.05)

            def do_insert_user(d):
                res = conn.execute(
                    text("""
                        INSERT INTO USERS (name_user, lastname, address, email, phone, city, country) 
                        VALUES (:name_user, :lastname, :address, :email, :phone, :city, :country)
                        RETURNING user_id
                    """),
                    d
                )
                return res.fetchone()[0]

            uid = do_insert_user(user_data)
            user_ids.append(uid)
            
            if should_duplicate:
                do_insert_user(user_data)

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
                cat_data = {
                    'name': cat['name'],
                    'description': cat['description']
                }
                
                # Aplicar corrupción 1% en name_category
                cat_data, _ = apply_corruption(cat_data, fields=['name'], prob=0.01)
                
                result = conn.execute(
                    text("""
                        INSERT INTO categories (name_category, description)
                        VALUES (:name, :description)
                        RETURNING category_id
                    """),
                    cat_data
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
                prov_data = {
                    'name': prov['name'],
                    'address': prov['address'],
                    'email': prov['email'],
                    'status': 'active'
                }
                
                # Aplicar corrupción 3% en email, status
                prov_data, _ = apply_corruption(prov_data, fields=['email', 'status'], prob=0.03)
                
                result = conn.execute(
                    text("""
                        INSERT INTO providers (name_provider, address, email, status)
                        VALUES (:name, :address, :email, :status)
                        RETURNING provider_id
                    """),
                    prov_data
                )
                provider_map[prov['name']] = result.fetchone()[0]
            
        conn.commit()
        log.info(f'PROVIDERS listos — {len(provider_map)} registros en total')
        return provider_map
    except Exception as ex:
        conn.rollback()
        log.error(f'ERROR en insert_providers: {ex}', exc_info=True)
        return {}


def insert_products(conn, category_ids: dict, provider_ids: dict, volume=10):
    try:
        log.info(f'Iniciando creacion/actualizacion de PRODUCTS (Lote: {volume})')
        with open(DATA_DIR / 'products.json', 'r', encoding='utf-8') as f:
            products_list = json.load(f)
            
        # Obtener existentes para evitar duplicados por código
        result = conn.execute(text("SELECT code FROM products"))
        existing_codes = {row[0] for row in result.fetchall()}
        
        products_to_insert = []
        
        # Si la base de datos está vacía, usamos un lote inicial base (ej: 200)
        # Si no, usamos el volumen ponderado que viene de main.py
        limit = 200 if not existing_codes else volume
        
        count = 0
        for p in products_list:
            if count >= limit:
                break
                
            code = p.get('code')
            if code in existing_codes:
                continue

            cat_name = p.get('category')
            prov_name = p.get('provider')
            cat_id = category_ids.get(cat_name)
            prov_id = provider_ids.get(prov_name)
            
            if cat_id is None or prov_id is None:
                continue

            product_data = {
                'name': p['name'],
                'category_id': cat_id,
                'provider_id': prov_id,
                'code': code,
                'cost_price': p['cost_price'],
                'sales_price': p['sales_price'],
                'stock': p['stock'],
                'status': 'active'
            }
            
            # Aplicar corrupción 5% en stock, precios, status
            product_data, _ = apply_corruption(product_data, fields=['stock', 'cost_price', 'sales_price', 'status'], prob=0.05)
            
            products_to_insert.append(product_data)
            count += 1
            
        if products_to_insert:
            log.info(f'Insertando {len(products_to_insert)} nuevos productos al catálogo.')
            conn.execute(
                text("""
                    INSERT INTO products 
                        (name_product, category_id, provider_id, code,
                         cost_price, sales_price, stock, status)
                    VALUES 
                        (:name, :category_id, :provider_id, :code,
                         :cost_price, :sales_price, :stock, :status)
                """),
                products_to_insert
            )
            
        conn.commit()
        
        # Retornar mapeo de todos los productos (existentes + nuevos) para las órdenes
        result = conn.execute(text("SELECT products_id, sales_price FROM products WHERE status = 'active'"))
        final_price_map = {row[0]: float(row[1]) for row in result.fetchall()}
        
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
        
        if not user_ids:
            log.error("No hay usuarios disponibles para crear ordenes.")
            return []

        orders_to_insert = []
        for _ in range(volume):
            user_id = random.choice(user_ids)
            status = random.choices(
                ['completed', 'processing', 'cancelled', 'pending'],
                weights=[0.5, 0.3, 0.1, 0.1],
                k=1
            )[0]
            
            order_data = {
                'user_id': user_id,
                'shipping_cost': round(random.uniform(5.0, 20.0), 2),
                'total_amount': 0.0,
                'status': status
            }
            
            # Aplicar corrupción 2% en shipping_cost, status
            order_data, _ = apply_corruption(order_data, fields=['shipping_cost', 'status'], prob=0.02)
            
            orders_to_insert.append(order_data)
            
        if orders_to_insert:
            order_ids = []
            for order_data in orders_to_insert:
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
        return []
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

        details_to_insert = []
        order_totals = {}

        for order_id in order_ids:
            num_items = random.randint(1, 5)
            order_total = 0.0
            
            selected_products = random.sample(product_ids, min(num_items, len(product_ids)))
            
            for p_id in selected_products:
                qty = random.randint(1, 10)
                price = product_price_map[p_id]
                order_total += (qty * price)
                
                detail_data = {
                    'products_id': p_id,
                    'order_id': order_id,
                    'quantity': qty,
                    'unit_price': price
                }
                
                # Aplicar corrupción 1% en quantity, unit_price
                detail_data, _ = apply_corruption(detail_data, fields=['quantity', 'unit_price'], prob=0.01)
                
                details_to_insert.append(detail_data)
            
            order_totals[order_id] = order_total
            
        if details_to_insert:
            conn.execute(
                text("""
                    INSERT INTO ORDERS_DETAILS (products_id, order_id, quantity, unit_price)
                    VALUES (:products_id, :order_id, :quantity, :unit_price)
                """),
                details_to_insert
            )
            
            # Actualizar totales en lote
            for oid, total in order_totals.items():
                conn.execute(
                    text("UPDATE ORDERS SET total_amount = :total_amount WHERE orders_id = :order_id"),
                    {'total_amount': total, 'order_id': oid}
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
            
            payment_data, _ = apply_corruption(payment_data, fields=['payment_method', 'status'], prob=0.02)
            
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

def simulate_ecommerce_updates(conn, volume=5):
    """
    Actualiza aleatoriamente algunos pedidos o usuarios para verificar triggers de updated_at.
    """
    try:
        log.info(f'Simulando {volume} actualizaciones en Ecommerce para updated_at')
        
        # 1. Actualizar estados de pedidos (avanzar algunos)
        result = conn.execute(text("SELECT orders_id FROM ORDERS WHERE status = 'processing' ORDER BY RANDOM() LIMIT :vol"), {'vol': volume})
        order_ids = [row[0] for row in result.fetchall()]
        
        for oid in order_ids:
            conn.execute(
                text("UPDATE ORDERS SET status = 'completed' WHERE orders_id = :id"),
                {'id': oid}
            )
            
        # 2. Actualizar direcciones de usuarios
        result = conn.execute(text("SELECT user_id FROM USERS ORDER BY RANDOM() LIMIT :vol"), {'vol': max(1, volume // 2)})
        for row in result.fetchall():
            new_address = faker.address().replace('\n', ', ')
            conn.execute(
                text("UPDATE USERS SET address = :address WHERE user_id = :id"),
                {'address': new_address, 'id': row[0]}
            )
            
        conn.commit()
        log.info('Simulación de actualizaciones en Ecommerce completada')
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en simulate_ecommerce_updates: {ex}')