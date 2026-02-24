from generators.config.config_generators import volumen
from utils.logging import logs
from faker import Faker
from sqlalchemy import text
import random


volume = volumen()
faker = Faker('es_CO')
log = logs()

def insert_users(conn):
    try:
        users = []
        for _ in range(volume["USERS"]):
            # Sacamos los nombres
            first_name = faker.first_name()
            last_name = faker.last_name()
            # aca concatenamos el nombre con el apellido
            email_name = f"{first_name}.{last_name}".lower()
            #aca se une el nombre con el dominio del email
            email = f"{email_name}@{faker.free_email_domain()}"
            # con esto sacamos las ciudades de colombia
            city = faker.city()
            country = 'Colombia'
            # agregamos los datos a la lista vacia
            users.append({
                'name_user':first_name,
                'lastname':last_name,
                'address':faker.address().replace('\n', ', '),
                'email':email,   
                'phone':faker.phone_number().replace(' ', ''),
                'city':city,
                'country':country   
            })

        conn.execute(text("TRUNCATE TABLE USERS RESTART IDENTITY CASCADE;"))
        conn.execute(
            text("""
                INSERT INTO USERS (name_user, lastname, address, email, phone, city, country) 
                VALUES (:name_user, :lastname, :address, :email, :phone, :city, :country)
                RETURNING user_id
            """),
            users
        )
        # ¡ESTA ES LA CLAVE!
        conn.commit()
        log.info('Insert Successfuly')
        
    except Exception as ex:
        log.error(f'ERROR en insert_users: {ex}',exc_info=True)