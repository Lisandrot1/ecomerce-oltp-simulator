# Documentación del Modelo de Datos (OLTP)

Este documento describe la estructura y el propósito de las tablas en las tres bases de datos que componen el simulador de E-commerce.

## 1. Base de Datos: Ecommerce (`db_ecommerce`)
Esta base de datos gestiona el núcleo del negocio: usuarios, productos, proveedores y ventas.

- **USERS**: Información de los clientes (nombre, correo, dirección, teléfono, ciudad, país). Es la fuente principal de datos de usuario y donde se simula la entrada de datos sucios.
- **PROVIDERS**: Empresas que suministran los productos. Incluye datos de contacto y estado operativo.
- **CATEGORIES**: Clasificación lógica de los productos (ej. Electrónica, Hogar, Ropa).
- **PRODUCTS**: El catálogo de artículos a la venta. Vincula categorías y proveedores. Incluye control de stock, precios de costo y venta, y estado del producto.
- **ORDERS**: Cabecera de los pedidos realizados por los usuarios. Almacena el usuario que compra, el costo de envío, el monto total y el estado del pedido (ej. pendiente, completado).
- **ORDERS_DETAILS**: Detalle de cada pedido. Vincula productos específicos con órdenes, registrando la cantidad comprada y el precio unitario en ese momento.
- **PAYMENTS**: Registro de transacciones financieras vinculadas a las órdenes, incluyendo el método de pago (tarjeta, efectivo, etc.) y el estado del pago.

## 2. Base de Datos: RRHH (`db_rrhh`)
Gestiona el talento humano y la estructura organizativa de la empresa.

- **DEPARTMENTS**: Departamentos internos de la empresa con su respectivo presupuesto anual y ubicación física.
- **POSITIONS**: Definición de cargos con sus niveles (Junior, Senior, etc.) y rangos salariales permitidos.
- **EMPLOYEES**: Información detallada de los trabajadores. Vincula a cada persona con su departamento y cargo. Incluye una relación de jerarquía (manager_id) y estado laboral.
- **ATTENDANCE**: Registro diario de asistencia, calculando las horas trabajadas y registrando entradas/salidas.
- **PAYROLL**: Gestión de nómina mensual, detallando sueldo base, bonos extra, deducciones de ley y el neto pagado.
- **PERFORMANCE**: Evaluaciones periódicas de desempeño con puntajes y comentarios, vinculando al evaluado con su evaluador.

## 3. Base de Datos: Marketing (`db_marketing`)
Gestiona la captación de clientes, campañas y estrategias de segmentación.

- **CAMPAIGNS**: Definición de campañas publicitarias, sus canales (social media, email), presupuestos y gastos reales. Vinculada a un responsable en RRHH.
- **LEADS**: Prospectos capturados a través de formularios. Es el punto crítico donde se simula la entrada de datos incompletos o erróneos por parte de interesados externos.
- **CUSTOMER_SEGMENTS**: Reglas para agrupar clientes según su volumen de compra o comportamiento.
- **CUSTOMER_SEGMENT_ASSIGNMENT**: Asignación histórica de usuarios de Ecommerce a los segmentos definidos.
- **PROMOTIONS**: Ofertas especiales y descuentos configurados para productos específicos por tiempo limitado.
- **EMAIL_CAMPAIGN_EVENTS**: Seguimiento técnico de interacciones con correos electrónicos (aperturas, clics), vinculado a usuarios de Ecommerce.

---
*Nota: Todas las tablas cuentan con triggers de auditoría para actualizar automáticamente el campo `updated_at` ante cualquier cambio.*
