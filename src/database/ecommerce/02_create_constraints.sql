-- 1. Relaciones de PRODUCTOS
ALTER TABLE PRODUCTS
    ADD CONSTRAINT fk_products_category 
    FOREIGN KEY (category_id) REFERENCES CATEGORIES(category_id),
    ADD CONSTRAINT fk_products_provider 
    FOREIGN KEY (provider_id) REFERENCES PROVIDERS(provider_id);

-- 2. Relaciones de ORDENES
ALTER TABLE ORDERS
    ADD CONSTRAINT fk_orders_users
    FOREIGN KEY (user_id) REFERENCES USERS(user_id);

-- 3. Relaciones de DETALLES DE ORDEN
ALTER TABLE ORDERS_DETAILS
    ADD CONSTRAINT fk_orderdetails_order
    FOREIGN KEY (order_id) REFERENCES ORDERS(orders_id), -- Apunta a orders_id (PK de ORDERS)
    ADD CONSTRAINT fk_orderdetails_products
    FOREIGN KEY (products_id) REFERENCES PRODUCTS(products_id);

-- 4. Relaciones de PAGOS
ALTER TABLE PAYMENTS
    ADD CONSTRAINT fk_payments_orders
    FOREIGN KEY (order_id) REFERENCES ORDERS(orders_id); -- Apunta a orders_id (PK de ORDERS)