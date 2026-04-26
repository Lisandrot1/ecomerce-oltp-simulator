from utils.logging import logs
from utils.db import get_connection
import generators.ecommerce as eco
import generators.rrhh as rrhh
import generators.marketing as mkt
import random
import datetime
from dotenv import load_dotenv

load_dotenv()

log = logs(__name__)

def get_hourly_weight():
    """
    Returns a weight (0.1 to 1.0) based on the current hour to simulate 
    realistic daily activity cycles.
    """
    hour = datetime.datetime.now().hour
    
    # 00:00 - 05:00 -> Very Low (10-15%)
    if 0 <= hour <= 5:
        return random.uniform(0.10, 0.15)
    # 06:00 - 08:00 -> Ramping up (30-60%)
    elif 6 <= hour <= 8:
        return random.uniform(0.30, 0.60)
    # 09:00 - 17:00 -> Peak (80-100%)
    elif 9 <= hour <= 17:
        return random.uniform(0.80, 1.00)
    # 18:00 - 21:00 -> Evening (50-70%)
    elif 18 <= hour <= 21:
        return random.uniform(0.50, 0.70)
    # 22:00 - 23:00 -> Ramping down (20-30%)
    else:
        return random.uniform(0.20, 0.30)

def main_ecommerce():
    try:
        weight = get_hourly_weight()
        with get_connection('ecommerce').connect() as conn:
            log.info('=' * 50)
            log.info(f'RUNNING ECOMMERCE GENERATOR (Weight: {weight:.2f})')

            product_price_map = eco.get_product_price_map(conn)
            
            if not product_price_map:
                log.info("Inicializando metadata (Categorías, Proveedores, Productos)...")
                category_ids = eco.insert_categories(conn)
                provider_ids = eco.insert_providers(conn)
                product_price_map = eco.insert_products(conn, category_ids, provider_ids)

            # USUARIOS NUEVOS
            user_vol = int(1000 * weight) # Meta: ~50k/día acumulados con 89 runs
            if user_vol > 0:
                eco.insert_users(conn, volume=user_vol)

            # ÓRDENES
            all_user_ids = eco.get_all_user_ids(conn)
            if all_user_ids and product_price_map:
                order_vol = int(5000 * weight) # Meta: ~300k/día acumulados con 89 runs
                if order_vol > 0:
                    order_ids = eco.insert_orders(conn, all_user_ids, volume=order_vol)
                    eco.insert_order_details(conn, order_ids, product_price_map)
                    eco.insert_payments(conn, order_ids)

            # SIMULACIÓN DE ACTUALIZACIONES (Para updated_at)
            eco.simulate_ecommerce_updates(conn, volume=int(10 * weight) + 1)

            log.info('ECOMMERCE GENERATOR FINISHED')
            log.info('=' * 50)
            return all_user_ids, list(product_price_map.keys())
    except Exception as ex:
        log.error(f'ERROR in main_ecommerce: {ex}', exc_info=True)
        return [], []

def main_rrhh():
    try:
        weight = get_hourly_weight()
        # RRHH usually has less dynamic hourly changes, but we apply a smaller weight variation
        # Or peak during business hours
        with get_connection('rrhh').connect() as conn:
            log.info('=' * 50)
            log.info(f'RUNNING RRHH GENERATOR (Weight: {weight:.2f})')
            
            dept_ids = rrhh.insert_departments(conn)
            pos_ids = rrhh.insert_positions(conn)
            
            # Employees and other RRHH data are less frequent
            emp_vol = int(5 * weight) # Meta: ~300 empleados/día acumulado
            if emp_vol > 0:
                emp_ids = rrhh.insert_employees(conn, dept_ids, pos_ids, volume=emp_vol)
                rrhh.insert_attendance(conn, emp_ids, days=1) # Daily attendance
                rrhh.insert_payroll(conn, emp_ids)
                rrhh.insert_performance(conn, emp_ids)
            
            all_emp_ids = rrhh.get_all_employee_ids(conn)

            # SIMULACIÓN DE ACTUALIZACIONES (Para updated_at)
            if all_emp_ids:
                rrhh.simulate_rrhh_updates(conn, volume=int(5 * weight) + 1)
            
            log.info('RRHH GENERATOR FINISHED')
            log.info('=' * 50)
            return all_emp_ids
    except Exception as ex:
        log.error(f'ERROR in main_rrhh: {ex}', exc_info=True)
        return []

def main_marketing(user_ids, employee_ids, product_ids):
    try:
        weight = get_hourly_weight()
        with get_connection('marketing').connect() as conn:
            log.info('=' * 50)
            log.info(f'RUNNING MARKETING GENERATOR (Weight: {weight:.2f})')
            
            camp_vol = int(1 * weight) 
            # ASEGURAMOS QUE EXISTAN CAMPAÑAS (Idempotente)
            camp_ids = mkt.insert_campaigns(conn, employee_ids=employee_ids, volume=max(1, camp_vol))
            
            if not camp_ids:
                # Si falló la creación, intentamos obtener las existentes por si acaso
                result = conn.execute(text("SELECT campaign_id FROM CAMPAIGNS"))
                camp_ids = [row[0] for row in result.fetchall()]
                
            lead_vol = int(1000 * weight) # Meta: ~50k/día acumulado
            if lead_vol > 0 and camp_ids:
                mkt.insert_leads(conn, camp_ids, user_ids=user_ids, volume=lead_vol)
            
            seg_ids = mkt.insert_customer_segments(conn)
            
            assign_vol = int(100 * weight)
            if assign_vol > 0:
                mkt.insert_segment_assignments(conn, user_ids, seg_ids, volume=assign_vol)
            
            # PROMOTIONS (Inicialización garantizada + Crecimiento ocasional)
            # Siempre las llamamos; la función interna maneja si ya existen
            mkt.insert_promotions(conn, product_ids=product_ids)
            
            event_vol = int(3000 * weight) # Meta: ~150k/día acumulado
            if event_vol > 0 and camp_ids:
                mkt.insert_campaign_events(conn, camp_ids, user_ids, volume=event_vol)
            
            # SIMULACIÓN DE ACTUALIZACIONES (Para updated_at)
            mkt.simulate_marketing_updates(conn, volume=int(10 * weight) + 1)
            
            log.info('MARKETING GENERATOR FINISHED')
            log.info('=' * 50)
    except Exception as ex:
        log.error(f'ERROR in main_marketing: {ex}', exc_info=True)

if __name__ == "__main__":
    # 1. Correr RRHH primero para obtener empleados (Managers/Responsables)
    all_employee_ids = main_rrhh()
    
    # 2. Correr Ecommerce
    all_user_ids, all_product_ids = main_ecommerce()
    
    # 3. Correr Marketing (necesita users, empleados y productos para sus vínculos lógicos)
    if True: # Siempre corremos marketing
        main_marketing(all_user_ids, all_employee_ids, all_product_ids)
    else:
        log.warning("Skipping Marketing Generator due to missing predecessor data.")