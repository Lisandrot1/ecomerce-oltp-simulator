from utils.logging import logs
from utils.db import get_connection
import generators.ecommerce as eco
import generators.rrhh as rrhh
import generators.marketing as mkt
import random

log = logs(__name__)

def main_ecommerce(employee_ids=None):
    try:
        with get_connection('ecommerce').connect() as conn:
            log.info('=' * 50)
            log.info('RUNNING ECOMMERCE GENERATOR')

            product_price_map = eco.get_product_price_map(conn)
            
            if not product_price_map:
                log.info("Inicializando metadata (Categorías, Proveedores, Productos)...")
                category_ids = eco.insert_categories(conn)
                provider_ids = eco.insert_providers(conn)
                product_price_map = eco.insert_products(conn, category_ids, provider_ids)

            # USUARIOS NUEVOS (Proporcional para llegar a 100k - 1M acumulados)
            eco.insert_users(conn, volume=20000)

            # ÓRDENES (Target: 80k por ejecución para llegar a 500k pronto)
            all_user_ids = eco.get_all_user_ids(conn)
            if all_user_ids and product_price_map:
                order_volume = 80000
                order_ids = eco.insert_orders(conn, all_user_ids, employee_ids=employee_ids, volume=order_volume)
                eco.insert_order_details(conn, order_ids, product_price_map)
                eco.insert_payments(conn, order_ids)

            log.info('ECOMMERCE GENERATOR FINISHED')
            log.info('=' * 50)
            return all_user_ids, list(product_price_map.keys())
    except Exception as ex:
        log.error(f'ERROR in main_ecommerce: {ex}', exc_info=True)
        return [], []

def main_rrhh():
    try:
        with get_connection('rrhh').connect() as conn:
            log.info('=' * 50)
            log.info('RUNNING RRHH GENERATOR')
            
            dept_ids = rrhh.insert_departments(conn)
            pos_ids = rrhh.insert_positions(conn)
            emp_ids = rrhh.insert_employees(conn, dept_ids, pos_ids, volume=200)
            rrhh.insert_attendance(conn, emp_ids, days=15)
            rrhh.insert_payroll(conn, emp_ids)
            rrhh.insert_performance(conn, emp_ids)
            
            # Recuperar todos los IDs para que el random sea más variado
            all_emp_ids = rrhh.get_all_employee_ids(conn)
            
            log.info('RRHH GENERATOR FINISHED')
            log.info('=' * 50)
            return all_emp_ids
    except Exception as ex:
        log.error(f'ERROR in main_rrhh: {ex}', exc_info=True)
        return []

def main_marketing(user_ids, employee_ids, product_ids):
    try:
        with get_connection('marketing').connect() as conn:
            log.info('=' * 50)
            log.info('RUNNING MARKETING GENERATOR')
            
            camp_ids = mkt.insert_campaigns(conn, employee_ids=employee_ids, volume=50)
            # Pasamos user_ids a leads para simular conversión
            mkt.insert_leads(conn, camp_ids, user_ids=user_ids, volume=20000)
            seg_ids = mkt.insert_customer_segments(conn)
            mkt.insert_segment_assignments(conn, user_ids, seg_ids, volume=5000)
            # Pasamos product_ids a promociones
            mkt.insert_promotions(conn, product_ids=product_ids, volume=20)
            mkt.insert_campaign_events(conn, camp_ids, user_ids, volume=50000)
            
            log.info('MARKETING GENERATOR FINISHED')
            log.info('=' * 50)
    except Exception as ex:
        log.error(f'ERROR in main_marketing: {ex}', exc_info=True)

if __name__ == "__main__":
    # 1. Correr RRHH primero para obtener empleados (Managers/Responsables)
    all_employee_ids = main_rrhh()
    
    # 2. Correr Ecommerce (necesita empleados para las ordenes)
    all_user_ids, all_product_ids = main_ecommerce(employee_ids=all_employee_ids)
    
    # 3. Correr Marketing (necesita users, empleados y productos para sus vínculos lógicos)
    if all_user_ids and all_employee_ids and all_product_ids:
        main_marketing(all_user_ids, all_employee_ids, all_product_ids)
    else:
        log.warning("Skipping Marketing Generator due to missing predecessor data.")