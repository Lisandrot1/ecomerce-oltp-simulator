# 🛒 E-commerce OLTP Simulator

Simulador de base de datos transaccional de un e-commerce real. Genera datos ficticios pero coherentes usando Python + Faker sobre PostgreSQL, pensado como fuente de datos para un pipeline ELT diario hacia una capa analítica (OLAP).

> **Este es el Proyecto 1 de 2.** El Proyecto 2 extrae esta data diariamente hacia BigQuery usando dbt para transformarla en un Star Schema analítico.

---

## 🎯 Objetivo

Simular el comportamiento transaccional de un e-commerce (usuarios, productos, órdenes, pagos) generando volúmenes realistas de datos para ser consumidos por un pipeline de datos diario.
```
[Este proyecto]                        [Proyecto 2]
Python + Faker → PostgreSQL (OLTP) → Pipeline ELT → BigQuery (OLAP) → Looker Studio
```

---

## 🏗️ Arquitectura
```
ecommerce-oltp-simulator/
│
├── docker-compose.yml           # PostgreSQL local para desarrollo
├── .env.example                 # Variables de entorno requeridas
│
├── database/
│   ├── ddl/
│   │   ├── 01_create_tables.sql
│   │   ├── 02_create_constraints.sql
│   │   └── 03_create_indexes.sql
│
└── src/
    ├── main.py
    ├── data/
    │   ├── categories.json
    │   ├── providers.json
    │   └── products.json
    ├── generators/
    │   └── ecommerce.py
    └── utils/
        ├── db.py
        └── logging.py
```

---

## 🗄️ Modelo de Datos
```
providers (30)     categories (20)
     │                   │
     └──────┬────────────┘
            │
         products (100)
            │
users ──── orders ──── order_details
                │
            payments
```

| Tabla | Desarrollo (seed) | Producción (diario) |
|---|---|---|
| users | 50,000 | 100 |
| products | 100 | 0 (fijos) |
| orders | 100,000 | 2,000 |
| order_details | ~250,000 | ~5,000 |
| payments | ~70,000 | ~1,400 |

---

## ⚙️ Requisitos

- Docker Desktop
- Python 3.12+
- PostgreSQL client (pgAdmin, o psql)

---

## 🚀 Instalación y uso

### 1. Clonar el repositorio
```bash
git clone [https://github.com/tu-usuario/ecommerce-oltp-simulator.git](https://github.com/Lisandrot1/ecomerce-oltp-simulator.git)
cd ecommerce-oltp-simulator
```

### 2. Configurar variables de entorno
```bash
cp .env.example .env
```

Edita `.env` con tus valores:
```env
ENV=development
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres123
DATABASE=ecommerce_oltp
DB_HOST=db
DB_PORT=5432


### 3. Levantar PostgreSQL con Docker
```bash
docker compose up -d
docker ps  # verificar que esté corriendo
```

### 4. Instalar dependencias Python
```bash
pip install -r requirements.txt
```

### 5. Correr el generador
```bash
docker compose run --rm ecomerce
```

Verás logs como:
```
INFO | Iniciando Generador de E-commerce
INFO | Insert CATEGORIES exitoso — 20 registros
INFO | Insert PROVIDERS exitoso — 30 registros
INFO | Insert PRODUCTS exitoso — 100 registros
INFO | Insert USERS exitoso — 50,000 registros
INFO | Insert ORDERS exitoso — 100,000 registros
INFO | Insert ORDER DETAILS exitoso — 250,431 registros
INFO | Insert PAYMENTS exitoso — 70,012 registros
INFO | Generador de E-commerce finalizado
```

---


---

## 🔌 Conectar DBeaver / pgAdmin
```
Host:     localhost
Port:     5432
Database: ecommerce_oltp
User:     postgres
Password: postgres123
```

## 🛠️ Stack

| Herramienta | Uso |
|---|---|
| PostgreSQL 16 | Base de datos transaccional (OLTP) |
| Docker | Entorno de desarrollo local |
| Python 3.12 | Generación de datos |
| Faker | Datos ficticios realistas |
| SQLAlchemy | ORM y conexión a PostgreSQL |
| Neon | PostgreSQL serverless en producción |
| GitHub Actions | Scheduling del generador diario |
