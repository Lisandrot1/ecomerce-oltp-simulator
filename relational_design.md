# Logical Join Map (OLTP-to-OLAP)

This document describes how to join the three distributed databases in your future OLAP layer (Gold Layer). Even though they live in separate PostgreSQL services, they are designed with consistent logical keys.

## Domain Relationships

### 1. Ecommerce & RRHH
**Goal**: Connect sales with the human talent responsible for them.
- **Table**: `db_ecommerce.ORDERS`
- **Link**: `employee_id`
- **Joins with**: `db_rrhh.EMPLOYEES.employee_id`
- **Analysis**: Sales performance by department, top sellers, payroll vs. revenue.

### 2. Marketing & Ecommerce
**Goal**: Track customer conversion and promo efficiency.
- **Table**: `db_marketing.CUSTOMER_SEGMENT_ASSIGNMENT`
- **Link**: `user_id`
- **Joins with**: `db_ecommerce.USERS.user_id`
- **Table**: `db_marketing.PROMOTIONS`
- **Link**: `products_id`
- **Joins with**: `db_ecommerce.PRODUCTS.products_id`
- **Table**: `db_marketing.LEADS`
- **Link**: `email` (or `user_id` if converted)
- **Joins with**: `db_ecommerce.USERS.email`
- **Analysis**: Campaign ROI, promotional impact on specific product sales, lead conversion rate.

### 3. Marketing & RRHH
**Goal**: Measure marketing personnel performance.
- **Table**: `db_marketing.CAMPAIGNS`
- **Link**: `employee_id`
- **Joins with**: `db_rrhh.EMPLOYEES.employee_id`
- **Analysis**: Campaigns managed by employee, budget performance by marketing manager.

## Entity Map Summary

| Origin Domain | Origin Table | Origin Column | Target Domain | Target Table | Target Column | Type |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Ecommerce | `ORDERS` | `employee_id` | RRHH | `EMPLOYEES` | `employee_id` | Logical |
| Marketing | `CAMPAIGNS` | `employee_id` | RRHH | `EMPLOYEES` | `employee_id` | Logical |
| Marketing | `LEADS` | `user_id` | Ecommerce | `USERS` | `user_id` | Logical |
| Marketing | `LEADS` | `email` | Ecommerce | `USERS` | `email` | Logical |
| Marketing | `EVENTS` | `user_id` | Ecommerce | `USERS` | `user_id` | Logical |
| Marketing | `PROMOTIONS` | `products_id` | Ecommerce | `PRODUCTS` | `products_id` | Logical |

## Internal Integrity (Physical FKs)
Within each database, referential integrity is enforced physicaly. 
Example: You cannot create a `PRODUCTS` record if the `CATEGORY` does not exist in the Ecommerce DB.
