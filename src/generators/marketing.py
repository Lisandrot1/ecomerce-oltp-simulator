from utils.logging import logs
from faker import Faker
from sqlalchemy import text
import random
import json
from pathlib import Path
from datetime import datetime, timedelta

faker = Faker()
log = logs()

def apply_corruption(data, null_prob=0.10, duplicate_prob=0.10):
    """
    Aplica suciedad a los datos: nulos o duplicados.
    Retorna (data_modificada, debe_duplicar)
    """
    should_duplicate = False
    if random.random() < null_prob:
        # Seleccionar 1-2 campos para poner en NULL (evitando IDs de relación)
        keys = [k for k in data.keys() if not k.endswith('_id') and k not in ['created_at', 'updated_at']]
        if keys:
            for k in random.sample(keys, min(len(keys), random.randint(1, 2))):
                data[k] = None
                
    if random.random() < duplicate_prob:
        should_duplicate = True
        
    return data, should_duplicate

def relax_marketing_constraints(conn):
    """
    Asegura que las restricciones de la DB estén relajadas para permitir la simulación de datos sucios.
    """
    try:
        conn.execute(text("ALTER TABLE LEADS DROP CONSTRAINT IF EXISTS leads_email_key"))
        conn.execute(text("ALTER TABLE EMAIL_CAMPAIGN_EVENTS ALTER COLUMN event_date DROP NOT NULL"))
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.warning(f"No se pudo relajar las restricciones (posiblemente ya lo estén): {e}")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'

def load_metadata():
    with open(DATA_DIR / 'metadata_marketing.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def load_geo():
    with open(DATA_DIR / 'metadata_geo.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def insert_campaigns(conn, employee_ids=None, volume=5):
    try:
        log.info(f'Iniciando inserción/verificación de CAMPAIGNS (Marketing)')
        metadata = load_metadata()
        
        # Obtener existentes
        result = conn.execute(text("SELECT name_campaign, campaign_id FROM CAMPAIGNS"))
        campaign_map = {row[0]: row[1] for row in result.fetchall()}
        
        campaign_ids = []
        
        # Primero aseguramos que las campañas base de metadata existan
        for name in metadata['campaign_names']:
            if name in campaign_map:
                campaign_ids.append(campaign_map[name])
                continue

            start = faker.date_between(start_date='-1y', end_date='today')
            end = start + timedelta(days=random.randint(15, 60))
            budget = round(random.uniform(5000000, 50000000), 2)
            
            data = {
                'name': name,
                'channel': random.choice(metadata['channels']),
                'start': start,
                'end': end,
                'budget': budget,
                'spent': round(budget * random.uniform(0.1, 1.1), 2),
                'status': random.choice(['active', 'paused', 'completed']),
                'emp_id': random.choice(employee_ids) if employee_ids else None
            }
            
            result = conn.execute(
                text("""
                    INSERT INTO CAMPAIGNS (name_campaign, channel, start_date, end_date, budget, spent, status, employee_id)
                    VALUES (:name, :channel, :start, :end, :budget, :spent, :status, :emp_id)
                    RETURNING campaign_id
                """),
                data
            )
            cid = result.fetchone()[0]
            campaign_ids.append(cid)
            campaign_map[name] = cid

        conn.commit()
        log.info(f'CAMPAIGNS listas — {len(campaign_ids)} campañas en total')
        return campaign_ids
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_campaigns: {ex}', exc_info=True)
        return []

def insert_leads(conn, campaign_ids, user_ids=None, volume=50):
    try:
        relax_marketing_constraints(conn)
        log.info(f'Iniciando inserción de {volume} LEADS (Marketing)')
        if not campaign_ids:
            log.warning("No campaign_ids provided for insert_leads. Skipping.")
            return []
            
        metadata = load_metadata()
        geo_data = load_geo()
        email_domains = ['gmail.com', 'outlook.com', 'yahoo.com', 'icloud.com', 'protonmail.com', 'zoho.com', 'hotmail.com']
        
        leads_to_insert = []
        for _ in range(volume):
            first_name = faker.first_name()
            last_name = faker.last_name()
            domain = random.choice(email_domains)
            
            # Generamos datos base con aleatoriedad para evitar colisiones involuntarias
            email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999999)}@{domain}"
            phone = f"+{random.randint(1, 99)} {random.randint(100, 999)} {random.randint(1000, 9999)}"
            geo = random.choice(geo_data)
            uid = random.choice(user_ids) if user_ids and random.random() > 0.6 else None
            
            data = {
                'camp_id': random.choice(campaign_ids),
                'user_id': uid,
                'fname': first_name,
                'lname': last_name,
                'email': email,
                'phone': phone,
                'city': random.choice(geo['cities']),
                'country': geo['country'],
                'source': random.choice(metadata['lead_sources']),
                'status': random.choice(metadata['lead_statuses'])
            }
            
            # Aplicar corrupción 20% (10% nulos, 10% duplicados)
            data, should_duplicate = apply_corruption(data, 0.10, 0.10)
            
            leads_to_insert.append(data)
            if should_duplicate:
                leads_to_insert.append(data.copy())

        if leads_to_insert:
            conn.execute(
                text("""
                    INSERT INTO LEADS (campaign_id, user_id, first_name, last_name, email, phone, city, country, source, status)
                    VALUES (:camp_id, :user_id, :fname, :lname, :email, :phone, :city, :country, :source, :status)
                """),
                leads_to_insert
            )
            
        conn.commit()
        log.info(f'Insert LEADS exitoso: {len(leads_to_insert)} registros (incluyendo duplicados)')
        return [] # No retornamos IDs por performance
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_leads: {ex}', exc_info=True)
        return []

def insert_customer_segments(conn):
    try:
        log.info('Iniciando inserción/verificación de CUSTOMER_SEGMENTS (Marketing)')
        metadata = load_metadata()
        
        # Obtener existentes
        result = conn.execute(text("SELECT name_segment, segment_id FROM CUSTOMER_SEGMENTS"))
        segment_map = {row[0]: row[1] for row in result.fetchall()}
        
        segment_ids = []
        for s in metadata['segments']:
            if s['name'] in segment_map:
                segment_ids.append(segment_map[s['name']])
                continue

            result = conn.execute(
                text("""
                    INSERT INTO CUSTOMER_SEGMENTS (name_segment, min_purchases, max_purchases, description)
                    VALUES (:name, :min, :max, :desc)
                    RETURNING segment_id
                """),
                s
            )
            val = result.fetchone()[0]
            segment_ids.append(val)
            segment_map[s['name']] = val

        conn.commit()
        log.info(f'CUSTOMER_SEGMENTS listos — {len(segment_ids)} segmentos en total')
        return segment_ids
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_customer_segments: {ex}', exc_info=True)
        return []

def insert_segment_assignments(conn, user_ids, segment_ids, volume=30):
    try:
        log.info(f'Iniciando inserción de {volume} SEGMENT_ASSIGNMENTS (Marketing)')
        count = 0
        for _ in range(volume):
            if not user_ids: break
            data = {
                'user_id': random.choice(user_ids),
                'seg_id': random.choice(segment_ids),
                'date': faker.date_between(start_date='-6m', end_date='today')
            }
            
            # Aplicar corrupción 20% (10% nulos, 10% duplicados)
            data, should_duplicate = apply_corruption(data, 0.10, 0.10)

            def do_insert_assignment(d):
                conn.execute(
                    text("""
                        INSERT INTO CUSTOMER_SEGMENT_ASSIGNMENT (user_id, segment_id, assigned_date)
                        VALUES (:user_id, :seg_id, :date)
                    """),
                    d
                )
            
            do_insert_assignment(data)
            if should_duplicate:
                do_insert_assignment(data)
            count += 1
        conn.commit()
        log.info(f'Insert SEGMENT_ASSIGNMENTS exitoso: {count} registros')
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_segment_assignments: {ex}', exc_info=True)

def insert_promotions(conn, product_ids=None, volume=5):
    try:
        log.info(f'Iniciando inserción/verificación de PROMOTIONS (Marketing)')
        metadata = load_metadata()
        
        # Obtener existentes
        result = conn.execute(text("SELECT name_promotion, promotion_id FROM PROMOTIONS"))
        promo_map = {row[0]: row[1] for row in result.fetchall()}
        
        promo_ids = []
        for name in metadata['promotions']:
            if name in promo_map:
                promo_ids.append(promo_map[name])
                continue

            start = faker.date_between(start_date='-1m', end_date='today')
            end = start + timedelta(days=random.randint(7, 30))
            data = {
                'name': name,
                'disc': random.uniform(5, 30),
                'start': start,
                'end': end,
                'min': random.choice([0, 20000, 50000, 100000]),
                'prod_id': random.choice(product_ids) if product_ids else None,
                'status': 'active'
            }
            result = conn.execute(
                text("""
                    INSERT INTO PROMOTIONS (name_promotion, discount_percent, start_date, end_date, min_purchase_amount, products_id, status)
                    VALUES (:name, :disc, :start, :end, :min, :prod_id, :status)
                    RETURNING promotion_id
                """),
                data
            )
            pid = result.fetchone()[0]
            promo_ids.append(pid)
            promo_map[name] = pid

        conn.commit()
        log.info(f'PROMOTIONS listas — {len(promo_map)} registros en total')
        return list(promo_map.values())
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_promotions: {ex}', exc_info=True)
        return []

def simulate_marketing_updates(conn, volume=5):
    """
    Actualiza aleatoriamente algunos leads o campañas para verificar triggers de updated_at.
    """
    try:
        log.info(f'Simulando {volume} actualizaciones en Marketing para updated_at')
        
        # 1. Actualizar estados de Leads
        result = conn.execute(text("SELECT lead_id FROM LEADS ORDER BY RANDOM() LIMIT :vol"), {'vol': volume})
        lead_ids = [row[0] for row in result.fetchall()]
        
        for lid in lead_ids:
            new_status = random.choice(['contacted', 'qualified', 'converted'])
            conn.execute(
                text("UPDATE LEADS SET status = :status WHERE lead_id = :id"),
                {'status': new_status, 'id': lid}
            )
            
        # 2. Actualizar spent en Campañas
        result = conn.execute(text("SELECT campaign_id, spent FROM CAMPAIGNS ORDER BY RANDOM() LIMIT :vol"), {'vol': max(1, volume // 2)})
        for cid, spent in result.fetchall():
            new_spent = float(spent) + random.uniform(1000, 10000)
            conn.execute(
                text("UPDATE CAMPAIGNS SET spent = :spent WHERE campaign_id = :id"),
                {'spent': new_spent, 'id': cid}
            )
            
        conn.commit()
        log.info('Simulación de actualizaciones en Marketing completada')
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en simulate_marketing_updates: {ex}')

def insert_campaign_events(conn, campaign_ids, user_ids, volume=100):
    try:
        relax_marketing_constraints(conn)
        log.info(f'Iniciando inserción de {volume} CAMPAIGN_EVENTS (Marketing)')
        if not user_ids or not campaign_ids:
            log.warning("Missing user_ids or campaign_ids for campaign events. Skipping.")
            return

        metadata = load_metadata()
        events_to_insert = []
        for _ in range(volume):
            data = {
                'camp_id': random.choice(campaign_ids),
                'user_id': random.choice(user_ids),
                'type': random.choice(metadata['event_types']),
                'date': datetime.now() - timedelta(minutes=random.randint(1, 10000))
            }
            
            # Aplicar corrupción 20% (10% nulos, 10% duplicados)
            data, should_duplicate = apply_corruption(data, 0.10, 0.10)
            
            events_to_insert.append(data)
            if should_duplicate:
                events_to_insert.append(data.copy())

        if events_to_insert:
            conn.execute(
                text("""
                    INSERT INTO EMAIL_CAMPAIGN_EVENTS (campaign_id, user_id, event_type, event_date)
                    VALUES (:camp_id, :user_id, :type, :date)
                """),
                events_to_insert
            )
            
        conn.commit()
        log.info(f'Insert CAMPAIGN_EVENTS exitoso: {len(events_to_insert)} registros')
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_campaign_events: {ex}', exc_info=True)
