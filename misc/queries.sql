WITH fct_billing_events AS (
SELECT c.customer_id, c.name as dim_name, c.status as dim_customer_status, c.signup_date as dim_signup_date, s.end_date, s.subscription_id, b.event_id, b.event_type as dim_event_type, b.amount as m_amount, s.plan_id, b.event_date::date as dim_event_date, country as dim_country, CURRENT_DATE as ds
FROM billing_events b JOIN subscriptions s USING (subscription_id) JOIN customers c USING (customer_id) 
),
customer_revenue AS (
SELECT customer_id, SUM(m_amount) as revenue FROM fct_billing_events WHERE dim_event_type = 'payment_succeeded' GROUP BY customer_id
),
unique_customers AS (
SELECT f.customer_id, f.dim_customer_status, MAX(start_date) as start_date, MIN(dim_signup_date) as dim_signup_date FROM fct_billing_events f JOIN subscriptions s ON s.customer_id = f.customer_id GROUP BY f.customer_id, f.dim_customer_status
),
customer_lifetime AS (
SELECT customer_id, dim_signup_date as signup_date, start_date as last_active,
CASE WHEN dim_customer_status = 'active' THEN CURRENT_DATE - dim_signup_date
	ELSE start_date - dim_signup_date
	END as lifetime
FROM unique_customers
),
customer_ltv AS (
SELECT * from customer_revenue r JOIN customer_lifetime l USING (customer_id)
),
cohorts AS (
SELECT DATE_TRUNC('month', signup_date)::date FROM customers
)
SELECT * FROM fct_billing_events GROUP BY 


