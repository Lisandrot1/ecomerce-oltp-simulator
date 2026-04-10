from utils.logging import logs
from faker import Faker
from sqlalchemy import text
import random
import json
from pathlib import Path
from datetime import datetime, timedelta

faker = Faker()
log = logs()

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
        log.info(f'Iniciando inserción de {volume} LEADS (Marketing) desde metadata')
        metadata = load_metadata()
        geo_data = load_geo()
        email_domains = ['gmail.com', 'outlook.com', 'yahoo.com', 'icloud.com', 'protonmail.com', 'zoho.com', 'hotmail.com']
        
        # Obtener existentes
        result = conn.execute(text("SELECT email, phone FROM LEADS"))
        rows = result.fetchall()
        existing_emails = {row[0] for row in rows}
        existing_phones = {row[1] for row in rows}
        
        lead_ids = []
        
        count = 0
        while count < volume:
            first_name = faker.first_name()
            last_name = faker.last_name()
            domain = random.choice(email_domains)
            
            email = f"{first_name.lower()}.{last_name.lower()}@{domain}"
            while email in existing_emails:
                email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 9999)}@{domain}"
            
            phone = f"+{random.randint(1, 99)} {random.randint(100, 999)} {random.randint(1000, 9999)}"
            while phone in existing_phones:
                phone = f"+{random.randint(1, 99)} {random.randint(100, 999)} {random.randint(1000, 9999)}"
            
            geo = random.choice(geo_data)
            
            # Simular que algunos leads ya son usuarios (conversión)
            uid = random.choice(user_ids) if user_ids and random.random() > 0.7 else None
            
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
            result = conn.execute(
                text("""
                    INSERT INTO LEADS (campaign_id, user_id, first_name, last_name, email, phone, city, country, source, status)
                    VALUES (:camp_id, :user_id, :fname, :lname, :email, :phone, :city, :country, :source, :status)
                    RETURNING lead_id
                """),
                data
            )
            lead_ids.append(result.fetchone()[0])
            existing_emails.add(email)
            existing_phones.add(phone)
            count += 1
        conn.commit()
        log.info(f'Insert LEADS exitoso: {len(lead_ids)} registros')
        return lead_ids
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
            conn.execute(
                text("""
                    INSERT INTO CUSTOMER_SEGMENT_ASSIGNMENT (user_id, segment_id, assigned_date)
                    VALUES (:user_id, :seg_id, :date)
                """),
                data
            )
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
        log.info(f'PROMOTIONS listas — {len(promo_ids)} registros')
        return promo_ids
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_promotions: {ex}', exc_info=True)
        return []

def insert_campaign_events(conn, campaign_ids, user_ids, volume=100):
    try:
        log.info(f'Iniciando inserción de {volume} CAMPAIGN_EVENTS (Marketing) desde metadata')
        metadata = load_metadata()
        count = 0
        for _ in range(volume):
            if not user_ids or not campaign_ids: break
            data = {
                'camp_id': random.choice(campaign_ids),
                'user_id': random.choice(user_ids),
                'type': random.choice(metadata['event_types']),
                'date': datetime.now() - timedelta(minutes=random.randint(1, 10000))
            }
            conn.execute(
                text("""
                    INSERT INTO EMAIL_CAMPAIGN_EVENTS (campaign_id, user_id, event_type, event_date)
                    VALUES (:camp_id, :user_id, :type, :date)
                """),
                data
            )
            count += 1
        conn.commit()
        log.info(f'Insert CAMPAIGN_EVENTS exitoso: {count} registros')
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_campaign_events: {ex}', exc_info=True)
