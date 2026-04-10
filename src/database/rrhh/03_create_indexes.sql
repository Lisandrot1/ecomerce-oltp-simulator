CREATE INDEX idx_employees_dept ON EMPLOYEES(department_id);
CREATE INDEX idx_employees_pos ON EMPLOYEES(position_id);
CREATE INDEX idx_employees_email ON EMPLOYEES(email);

CREATE INDEX idx_attendance_employee_date ON ATTENDANCE(employee_id, date);

CREATE INDEX idx_payroll_employee_period ON PAYROLL(employee_id, period_start);

CREATE INDEX idx_performance_employee ON PERFORMANCE(employee_id);
