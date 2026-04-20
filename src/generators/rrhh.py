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

def load_rrhh_metadata():
    with open(DATA_DIR / 'metadata_rrhh.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def load_geo_metadata():
    with open(DATA_DIR / 'metadata_geo.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def insert_departments(conn):
    try:
        log.info('Iniciando inserción/verificación de DEPARTMENTS (RRHH)')
        metadata = load_rrhh_metadata()
        geo_data = load_geo_metadata()
        
        # Obtener existentes
        result = conn.execute(text("SELECT name_department, department_id FROM DEPARTMENTS"))
        dept_map = {row[0]: row[1] for row in result.fetchall()}
        
        dept_ids = []
        for d in metadata['departments']:
            if d['name'] in dept_map:
                dept_ids.append(dept_map[d['name']])
                continue

            geo = random.choice(geo_data)
            location = f"{random.choice(geo['cities'])}, {geo['country']}"
            data = {
                'name': d['name'],
                'location': location,
                'budget': round(random.uniform(d['budget_range'][0], d['budget_range'][1]), 2)
            }
            result = conn.execute(
                text("""
                    INSERT INTO DEPARTMENTS (name_department, location, budget)
                    VALUES (:name, :location, :budget)
                    RETURNING department_id
                """),
                data
            )
            val = result.fetchone()[0]
            dept_ids.append(val)
            dept_map[d['name']] = val

        conn.commit()
        log.info(f'DEPARTMENTS listos — {len(dept_ids)} departamentos en total')
        return dept_ids
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_departments: {ex}', exc_info=True)
        return []

def insert_positions(conn):
    try:
        log.info('Iniciando inserción/verificación de POSITIONS (RRHH)')
        metadata = load_rrhh_metadata()
        
        # Obtener existentes
        result = conn.execute(text("SELECT name_position, level, position_id FROM POSITIONS"))
        pos_map = {(row[0], row[1]): row[2] for row in result.fetchall()}

        pos_ids = []
        for p in metadata['positions']:
            key = (p['name'], p['level'])
            if key in pos_map:
                pos_ids.append(pos_map[key])
                continue

            data = {
                'name': p['name'],
                'level': p['level'],
                'min': p['salary_range'][0],
                'max': p['salary_range'][1]
            }
            result = conn.execute(
                text("""
                    INSERT INTO POSITIONS (name_position, level, min_salary, max_salary)
                    VALUES (:name, :level, :min, :max)
                    RETURNING position_id
                """),
                data
            )
            val = result.fetchone()[0]
            pos_ids.append(val)
            pos_map[key] = val

        conn.commit()
        log.info(f'POSITIONS listos — {len(pos_ids)} cargos en total')
        return pos_ids
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_positions: {ex}', exc_info=True)
        return []

def insert_employees(conn, dept_ids, pos_ids, volume=20):
    try:
        log.info(f'Iniciando inserción de {volume} EMPLOYEES (RRHH)')
        geo_data = load_geo_metadata()
        email_domains = ['gmail.com', 'outlook.com', 'yahoo.com', 'icloud.com', 'protonmail.com', 'zoho.com', 'hotmail.com']
        
        # Obtener existentes para evitar duplicados en la misma ejecución o ejecuciones seguidas
        result = conn.execute(text("SELECT email, phone FROM EMPLOYEES"))
        rows = result.fetchall()
        existing_emails = {row[0] for row in rows}
        existing_phones = {row[1] for row in rows}
        
        employee_ids = []
        
        count = 0
        while count < volume:
            first_name = faker.first_name()
            last_name = faker.last_name()
            domain = random.choice(email_domains)
            # Asegurar email único
            email = f"{first_name.lower()}.{last_name.lower()}@{domain}"
            while email in existing_emails:
                email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 9999)}@{domain}"
            
            # Asegurar teléfono único
            phone = f"+{random.randint(1, 99)} {random.randint(100, 999)} {random.randint(1000, 9999)}"
            while phone in existing_phones:
                phone = f"+{random.randint(1, 99)} {random.randint(100, 999)} {random.randint(1000, 9999)}"
                
            salary = round(random.uniform(1500000, 15000000), 2)
            
            emp_data = {
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': phone,
                'hire_date': faker.date_between(start_date='-6y', end_date='today'),
                'birth_date': faker.date_of_birth(minimum_age=20, maximum_age=60),
                'salary': salary,
                'department_id': random.choice(dept_ids),
                'position_id': random.choice(pos_ids),
                'status': random.choice(['Activo', 'Activo', 'Activo', 'Inactivo', 'Vacaciones'])
            }
            
            result = conn.execute(
                text("""
                    INSERT INTO EMPLOYEES (first_name, last_name, email, phone, hire_date, birth_date, salary, department_id, position_id, status)
                    VALUES (:first_name, :last_name, :email, :phone, :hire_date, :birth_date, :salary, :department_id, :position_id, :status)
                    RETURNING employee_id
                """),
                emp_data
            )
            eid = result.fetchone()[0]
            employee_ids.append(eid)
            existing_emails.add(email)
            existing_phones.add(phone)
            count += 1
            
        # Asignar manager_id aleatoriamente a algunos empleados (self-reference)
        # Recargamos IDs para incluir a los recién creados
        result = conn.execute(text("SELECT employee_id FROM EMPLOYEES"))
        all_ids = [row[0] for row in result.fetchall()]
        
        for emp_id in employee_ids:
            if random.random() > 0.3: # 70% tienen manager
                manager_id = random.choice(all_ids)
                if manager_id != emp_id:
                    conn.execute(
                        text("UPDATE EMPLOYEES SET manager_id = :manager_id WHERE employee_id = :employee_id"),
                        {'manager_id': manager_id, 'employee_id': emp_id}
                    )
        
        conn.commit()
        log.info(f'Insert EMPLOYEES exitoso: {len(employee_ids)} registros')
        return employee_ids
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_employees: {ex}', exc_info=True)
        return []

def insert_attendance(conn, employee_ids, days=5):
    try:
        log.info(f'Iniciando inserción de ATTENDANCE (RRHH) para {days} días')
        count = 0
        current_date = datetime.now().date()
        
        for i in range(days):
            date = current_date - timedelta(days=i)
            # Saltamos fines de semana (opcional, pero realista)
            if date.weekday() >= 5: continue
            
            for emp_id in employee_ids:
                # 90% de probabilidad de asistencia
                if random.random() < 0.9:
                    check_in = datetime.strptime(f"{random.randint(7, 9)}:{random.randint(0, 59)}", "%H:%M").time()
                    check_out = datetime.strptime(f"{random.randint(16, 18)}:{random.randint(0, 59)}", "%H:%M").time()
                    
                    # Cálculo simple de horas
                    h_in = check_in.hour + check_in.minute/60
                    h_out = check_out.hour + check_out.minute/60
                    hours = round(h_out - h_in, 2)
                    
                    data = {
                        'employee_id': emp_id,
                        'date': date,
                        'check_in': check_in,
                        'check_out': check_out,
                        'hours': hours,
                        'status': 'Presente'
                    }
                    conn.execute(
                        text("""
                            INSERT INTO ATTENDANCE (employee_id, date, check_in, check_out, hours_worked, status)
                            VALUES (:employee_id, :date, :check_in, :check_out, :hours, :status)
                        """),
                        data
                    )
                    count += 1
        conn.commit()
        log.info(f'Insert ATTENDANCE exitoso: {count} registros')
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_attendance: {ex}', exc_info=True)

def insert_payroll(conn, employee_ids):
    try:
        log.info('Iniciando inserción de PAYROLL (RRHH)')
        count = 0
        period_start = datetime.now().replace(day=1).date() - timedelta(days=30)
        period_end = datetime.now().replace(day=1).date() - timedelta(days=1)
        
        # Obtener salarios actuales
        result = conn.execute(text("SELECT employee_id, salary FROM EMPLOYEES WHERE status = 'Activo'"))
        rows = result.fetchall()
        
        for emp_id, salary in rows:
            salary_f = float(salary)
            bonuses = round(random.uniform(0, salary_f * 0.1), 2) if random.random() > 0.8 else 0
            deductions = round(salary_f * 0.08, 2) # 8% salud/pensión en Colombia
            total = round(salary_f + bonuses - deductions, 2)
            
            data = {
                'emp_id': emp_id,
                'start': period_start,
                'end': period_end,
                'base': salary,
                'bonuses': bonuses,
                'deductions': deductions,
                'total': total
            }
            conn.execute(
                text("""
                    INSERT INTO PAYROLL (employee_id, period_start, period_end, base_salary, bonuses, deductions, total_payment)
                    VALUES (:emp_id, :start, :end, :base, :bonuses, :deductions, :total)
                """),
                data
            )
            count += 1
        conn.commit()
        log.info(f'Insert PAYROLL exitoso: {count} registros')
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_payroll: {ex}', exc_info=True)

def insert_performance(conn, employee_ids):
    try:
        log.info('Iniciando inserción de PERFORMANCE (RRHH)')
        count = 0
        for emp_id in random.sample(employee_ids, int(len(employee_ids)*0.5)):
            reviewer_id = random.choice(employee_ids)
            if reviewer_id == emp_id: continue
            
            data = {
                'emp_id': emp_id,
                'date': faker.date_between(start_date='-1y', end_date='today'),
                'score': random.randint(3, 5),
                'reviewer': reviewer_id,
                'comments': faker.sentence(nb_words=10)
            }
            conn.execute(
                text("""
                    INSERT INTO PERFORMANCE (employee_id, review_date, score, reviewer_id, comments)
                    VALUES (:emp_id, :date, :score, :reviewer, :comments)
                """),
                data
            )
            count += 1
        conn.commit()
        log.info(f'Insert PERFORMANCE exitoso: {count} registros')
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en insert_performance: {ex}', exc_info=True)

def simulate_rrhh_updates(conn, volume=5):
    """
    Actualiza aleatoriamente algunos empleados para verificar triggers de updated_at.
    """
    try:
        log.info(f'Simulando {volume} actualizaciones en RRHH para updated_at')
        
        # 1. Actualizar salarios
        result = conn.execute(text("SELECT employee_id, salary FROM EMPLOYEES ORDER BY RANDOM() LIMIT :vol"), {'vol': volume})
        for eid, salary in result.fetchall():
            new_salary = float(salary) * random.uniform(1.02, 1.05)
            conn.execute(
                text("UPDATE EMPLOYEES SET salary = :salary WHERE employee_id = :id"),
                {'salary': new_salary, 'id': eid}
            )
            
        # 2. Actualizar estados
        result = conn.execute(text("SELECT employee_id FROM EMPLOYEES ORDER BY RANDOM() LIMIT :vol"), {'vol': max(1, volume // 2)})
        for row in result.fetchall():
            new_status = random.choice(['Activo', 'Vacaciones'])
            conn.execute(
                text("UPDATE EMPLOYEES SET status = :status WHERE employee_id = :id"),
                {'status': new_status, 'id': row[0]}
            )
            
        conn.commit()
        log.info('Simulación de actualizaciones en RRHH completada')
    except Exception as ex:
        conn.rollback()
        log.error(f'Error en simulate_rrhh_updates: {ex}')

def get_all_employee_ids(conn):
    try:
        result = conn.execute(text("SELECT employee_id FROM EMPLOYEES"))
        return [row[0] for row in result.fetchall()]
    except:
        return []
