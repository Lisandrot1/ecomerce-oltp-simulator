-- Function to update updated_at column automatically
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TABLE IF NOT EXISTS DEPARTMENTS (
    department_id SERIAL PRIMARY KEY,
    name_department VARCHAR(100) NOT NULL,
    location VARCHAR(100) NOT NULL,
    budget NUMERIC(15,2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_departments_updated_at BEFORE UPDATE ON DEPARTMENTS
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS POSITIONS (
    position_id SERIAL PRIMARY KEY,
    name_position VARCHAR(100) NOT NULL,
    level VARCHAR(50) NOT NULL,
    min_salary NUMERIC(12,2) NOT NULL,
    max_salary NUMERIC(12,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_positions_updated_at BEFORE UPDATE ON POSITIONS
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

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
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_employees_updated_at BEFORE UPDATE ON EMPLOYEES
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS ATTENDANCE (
    attendance_id SERIAL PRIMARY KEY,
    employee_id INT NOT NULL REFERENCES EMPLOYEES(employee_id),
    date DATE NOT NULL,
    check_in TIME,
    check_out TIME,
    hours_worked NUMERIC(5,2),
    status VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_attendance_updated_at BEFORE UPDATE ON ATTENDANCE
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS PAYROLL (
    payroll_id SERIAL PRIMARY KEY,
    employee_id INT NOT NULL REFERENCES EMPLOYEES(employee_id),
    period_start DATE,
    period_end DATE,
    base_salary NUMERIC(12,2),
    bonuses NUMERIC(12,2) DEFAULT 0.00,
    deductions NUMERIC(12,2) DEFAULT 0.00,
    total_payment NUMERIC(12,2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_payroll_updated_at BEFORE UPDATE ON PAYROLL
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS PERFORMANCE (
    performance_id SERIAL PRIMARY KEY,
    employee_id INT NOT NULL REFERENCES EMPLOYEES(employee_id),
    review_date DATE,
    score INT CHECK (score BETWEEN 1 AND 5),
    reviewer_id INT NOT NULL REFERENCES EMPLOYEES(employee_id),
    comments TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_performance_updated_at BEFORE UPDATE ON PERFORMANCE
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

