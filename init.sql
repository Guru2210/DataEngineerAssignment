-- ====================================================================
-- Part 1: Staging Schema for Airflow Pipeline
-- ====================================================================
-- This is where the PythonOperator (pandas.to_sql) will land the raw CSV data.
CREATE SCHEMA IF NOT EXISTS raw_staging;


-- ====================================================================
-- Part 2: Source System Schema (OLTP Simulation)
-- ====================================================================
-- This represents the Telco's backend transactional database.
CREATE SCHEMA IF NOT EXISTS source_system;

-- Set default search path for the following table creations
SET search_path TO source_system;

-- 1. Service Plans Catalog
CREATE TABLE IF NOT EXISTS service_plan (
    plan_id SERIAL PRIMARY KEY,
    plan_name VARCHAR(100) NOT NULL,
    monthly_fee DECIMAL(10, 2) NOT NULL,
    infrastructure_cost DECIMAL(10, 2) NOT NULL,
    data_limit_gb INT NOT NULL
);

-- 2. Customer Profiles
CREATE TABLE IF NOT EXISTS customer (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    gender VARCHAR(20),
    customer_segment VARCHAR(50),
    is_churned BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Subscription History (Junction table tracking lifecycle)
CREATE TABLE IF NOT EXISTS subscription (
    subscription_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    plan_id INT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE, -- NULL means currently active
    status VARCHAR(50) NOT NULL,
    CONSTRAINT fk_sub_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    CONSTRAINT fk_sub_plan FOREIGN KEY (plan_id) REFERENCES service_plan(plan_id)
);

-- 4. Network Usage Telemetry (High Volume / Append-Only)
CREATE TABLE IF NOT EXISTS network_usage (
    usage_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    usage_date DATE NOT NULL,
    data_consumed_mb DECIMAL(12, 2) DEFAULT 0.00,
    dropped_calls_count INT DEFAULT 0,
    average_latency_ms INT DEFAULT 0,
    CONSTRAINT fk_usage_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

-- 5. Billing & Invoicing (Monthly Transactions)
CREATE TABLE IF NOT EXISTS billing_invoice (
    invoice_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    subscription_id INT NOT NULL,
    billing_period_start DATE NOT NULL,
    billing_period_end DATE NOT NULL,
    amount_due DECIMAL(10, 2) NOT NULL,
    due_date DATE NOT NULL,
    payment_date DATE,
    is_delayed BOOLEAN DEFAULT FALSE,
    CONSTRAINT fk_billing_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    CONSTRAINT fk_billing_subscription FOREIGN KEY (subscription_id) REFERENCES subscription(subscription_id)
);

-- 6. Support Tickets (Optional: for further churn correlation)
CREATE TABLE IF NOT EXISTS support_ticket (
    ticket_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    issue_category VARCHAR(100),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolution_date TIMESTAMP,
    satisfaction_score INT CHECK (satisfaction_score >= 1 AND satisfaction_score <= 5),
    CONSTRAINT fk_ticket_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

-- Reset search path to default public schema
SET search_path TO public;