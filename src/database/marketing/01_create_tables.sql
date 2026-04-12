-- Function to update updated_at column automatically
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TABLE IF NOT EXISTS CAMPAIGNS (
    campaign_id SERIAL PRIMARY KEY,
    name_campaign VARCHAR(150) NOT NULL,
    channel VARCHAR(50) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    budget NUMERIC(15,2) NOT NULL DEFAULT 0.00,
    spent NUMERIC(15,2) NOT NULL DEFAULT 0.00,
    status VARCHAR(50) NOT NULL,
    employee_id INT, -- Logical relation to RRHH.EMPLOYEES (Campaign Manager)
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_campaigns_updated_at BEFORE UPDATE ON CAMPAIGNS
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS LEADS (
    lead_id SERIAL PRIMARY KEY,
    campaign_id INT REFERENCES CAMPAIGNS(campaign_id),
    user_id INT, -- Logical relation to ECOMMERCE.USERS (if converted)
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    source VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_leads_updated_at BEFORE UPDATE ON LEADS
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS CUSTOMER_SEGMENTS (
    segment_id SERIAL PRIMARY KEY,
    name_segment VARCHAR(100) NOT NULL,
    min_purchases INT DEFAULT 0,
    max_purchases INT,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_customer_segments_updated_at BEFORE UPDATE ON CUSTOMER_SEGMENTS
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS CUSTOMER_SEGMENT_ASSIGNMENT (
    assignment_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL, -- Logical relation to ECOMMERCE.USERS
    segment_id INT NOT NULL REFERENCES CUSTOMER_SEGMENTS(segment_id),
    assigned_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_customer_segment_assignment_updated_at BEFORE UPDATE ON CUSTOMER_SEGMENT_ASSIGNMENT
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS PROMOTIONS (
    promotion_id SERIAL PRIMARY KEY,
    name_promotion VARCHAR(150) NOT NULL,
    discount_percent NUMERIC(5,2) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    min_purchase_amount NUMERIC(12,2) DEFAULT 0.00,
    products_id INT, -- Logical relation to ECOMMERCE.PRODUCTS
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_promotions_updated_at BEFORE UPDATE ON PROMOTIONS
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS EMAIL_CAMPAIGN_EVENTS (
    event_id SERIAL PRIMARY KEY,
    campaign_id INT NOT NULL REFERENCES CAMPAIGNS(campaign_id),
    user_id INT NOT NULL, -- Logical relation to ECOMMERCE.USERS
    event_type VARCHAR(50) NOT NULL,
    event_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_email_campaign_events_updated_at BEFORE UPDATE ON EMAIL_CAMPAIGN_EVENTS
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

