CREATE TABLE IF NOT EXISTS DEPARTMENTS (
    department_id SERIAL PRIMARY KEY,
    name_department VARCHAR(100) NOT NULL,
    location VARCHAR(100) NOT NULL,
    budget NUMERIC(15,2) NOT NULL DEFAULT 0.00,
    datetime TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS POSITIONS (
    position_id SERIAL PRIMARY KEY,
    name_position VARCHAR(100) NOT NULL,
    level VARCHAR(50) NOT NULL,
    min_salary NUMERIC(12,2) NOT NULL,
    max_salary NUMERIC(12,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS EMPLOYEES (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20) NOT NULL,
    hire_date DATE NOT NULL,
    birth_date DATE NOT NULL,
    salary NUMERIC(12,2) NOT NULL,
    department_id INT NOT NULL REFERENCES DEPARTMENTS(department_id),
    position_id INT NOT NULL REFERENCES POSITIONS(position_id),
    manager_id INT REFERENCES EMPLOYEES(employee_id),
    status VARCHAR(50) NOT NULL,
    datetime TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ATTENDANCE (
    attendance_id SERIAL PRIMARY KEY,
    employee_id INT NOT NULL REFERENCES EMPLOYEES(employee_id),
    date DATE NOT NULL,
    check_in TIME,
    check_out TIME,
    hours_worked NUMERIC(5,2),
    status VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS PAYROLL (
    payroll_id SERIAL PRIMARY KEY,
    employee_id INT NOT NULL REFERENCES EMPLOYEES(employee_id),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    base_salary NUMERIC(12,2) NOT NULL,
    bonuses NUMERIC(12,2) DEFAULT 0.00,
    deductions NUMERIC(12,2) DEFAULT 0.00,
    total_payment NUMERIC(12,2) NOT NULL,
    datetime TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS PERFORMANCE (
    performance_id SERIAL PRIMARY KEY,
    employee_id INT NOT NULL REFERENCES EMPLOYEES(employee_id),
    review_date DATE NOT NULL,
    score INT CHECK (score BETWEEN 1 AND 5),
    reviewer_id INT NOT NULL REFERENCES EMPLOYEES(employee_id),
    comments TEXT,
    datetime TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
