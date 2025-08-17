CREATE SCHEMA IF NOT EXISTS saas;

CREATE TABLE IF NOT EXISTS saas.raw_billing_events (
    subscription_id STRING,
    event_id STRING,
    event_type STRING,
    amount DOUBLE,
    event_date DATE,
    ds DATE
)
USING iceberg
PARTITIONED BY (ds);

CREATE TABLE IF NOT EXISTS saas.raw_subscriptions (
    subscription_id STRING,
    customer_id STRING,
    plan_id STRING,
    end_date DATE,
    ds DATE
)
USING iceberg
PARTITIONED BY (ds);

CREATE TABLE IF NOT EXISTS saas.raw_customers (
    customer_id STRING,
    name STRING,
    status STRING,
    country STRING,
    signup_date DATE,
    ds DATE
)
USING iceberg
PARTITIONED BY (ds);

CREATE TABLE IF NOT EXISTS saas.stg_billing_events (
    subscription_id STRING,
    event_id STRING,
    event_type STRING,
    amount DOUBLE,
    event_date DATE,
    ds DATE
)
USING iceberg
PARTITIONED BY (event_date, bucket(16, subscription_id));

CREATE TABLE IF NOT EXISTS saas.stg_subscriptions (
    subscription_id STRING,
    customer_id STRING,
    plan_id STRING,
    end_date DATE,
    ds DATE
)
USING iceberg
PARTITIONED BY (bucket(16, subscription_id));

CREATE TABLE IF NOT EXISTS saas.dim_customers_scd2 (
    customer_id STRING,
    name STRING,
    status STRING,
    country STRING,
    signup_date DATE,
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN,
    ds DATE
)
USING iceberg
PARTITIONED BY (is_current);

CREATE TABLE IF NOT EXISTS saas.fct_billing_events (
    customer_id STRING,
    dim_name STRING,
    dim_customer_status STRING,
    dim_signup_date DATE,
    dim_subscription_end_date DATE,
    subscription_id STRING,
    event_id STRING,
    dim_event_type STRING,
    m_amount DOUBLE,
    plan_id STRING,
    dim_event_date DATE,
    dim_country STRING,
    month_start DATE,
    ds DATE
)
USING iceberg
PARTITIONED BY (dim_event_date);

-- stg table for write audit publish pattern
CREATE TABLE IF NOT EXISTS saas.fct_billing_events_staging (
    customer_id STRING,
    dim_name STRING,
    dim_customer_status STRING,
    dim_signup_date DATE,
    dim_subscription_end_date DATE,
    subscription_id STRING,
    event_id STRING,
    dim_event_type STRING,
    m_amount DOUBLE,
    plan_id STRING,
    dim_event_date DATE,
    dim_country STRING,
    month_start DATE,
    ds DATE
)
USING iceberg
PARTITIONED BY (ds);

CREATE TABLE IF NOT EXISTS saas.plans (
    plan_id STRING,
    name STRING,
    description STRING,
    price DOUBLE,
    billing_period STRING,
    ds DATE
)
USING iceberg
PARTITIONED BY (ds);

CREATE TABLE IF NOT EXISTS saas.agg_billing_analytics (
    aggregation_level STRING,
    dim_plan STRING,
    dim_country STRING,
    month_start DATE,
    m_total_events BIGINT,
    m_revenue DOUBLE,
    execution_date DATE
)
USING iceberg
PARTITIONED BY (month_start);
