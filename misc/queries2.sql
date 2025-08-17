WITH fct_billing_events AS (
SELECT c.customer_id, c.name as dim_name, c.status as dim_customer_status, c.signup_date as dim_signup_date, s.end_date, s.subscription_id, b.event_id, b.event_type as dim_event_type, b.amount as m_amount, s.plan_id, b.event_date::date as dim_event_date, country as dim_country, DATE_TRUNC('month', b.event_date)::date as month_start, CURRENT_DATE as ds
FROM billing_events b JOIN subscriptions s USING (subscription_id) JOIN customers c USING (customer_id) 
)
-- ,
-- cohort_month_agg AS (
-- SELECT 'cohort' as aggregation_level, DATE_TRUNC('month', dim_event_date)::date, COUNT(1), SUM(m_amount), AVG(m_amount), CURRENT_DATE as ds FROM fct_billing_events WHERE dim_event_type = 'payment_succeeded' GROUP BY 1,2 ORDER BY 2
-- ),
-- country_agg AS (
-- SELECT 'country' as aggregation_level, dim_country, COUNT(1), SUM(m_amount), AVG(m_amount), CURRENT_DATE as ds FROM fct_billing_events WHERE dim_event_type = 'payment_succeeded' GROUP BY 1,2
-- ),
-- plan_agg AS (
-- SELECT 'plan' as aggregation_level, p.name as dim_plan, COUNT(1), SUM(m_amount), AVG(m_amount), CURRENT_DATE as ds FROM fct_billing_events f JOIN plans p USING(plan_id) WHERE dim_event_type = 'payment_succeeded' GROUP BY 1,2
-- )


SELECT 
CASE WHEN GROUPING(p.name) = 1 AND GROUPING(dim_country) = 1 THEN 'overall' 
	WHEN GROUPING(p.name) = 0 AND GROUPING(dim_country) = 0 THEN 'plan_country'
	WHEN GROUPING(p.name) = 0 THEN 'plan'
	WHEN GROUPING(dim_country) = 0 THEN 'country'
END AS aggregation_level, 
COALESCE (p.name, 'overall') as dim_plan, COALESCE(dim_country, 'overall') as dim_country, month_start, COUNT(1) as m_total_events, SUM(m_amount) as m_revenue, NULL as CURRENT_DATE
FROM fct_billing_events JOIN plans p USING (plan_id) 
WHERE dim_event_type = 'payment_succeeded'
GROUP BY GROUPING SETS (
	(p.name, month_start),
	(dim_country, month_start),
	(p.name, dim_country, month_start),
	(month_start),
	()
)
ORDER BY aggregation_level




