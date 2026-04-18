/*
Последовательно рассчитаем производные метрики, которые лягут в основу логики финальных запросов для дашборда (конверсию, средний чек, количество позиций в заказе, Retention Rate по когортам, LTV на пользователя по когортам, ARPU, ARPPU, DAU)
*/
--1. Конверсия
SELECT
    ROUND(CAST(COUNT(DISTINCT customer_id) AS NUMERIC) / (SELECT CAST(COUNT(DISTINCT plf_participant_id) AS NUMERIC) FROM plf_participants), 1) AS conversion
FROM
	customer_purchases AS cp
INNER JOIN
	plf_participants AS pp ON cp.customer_id = pp.plf_participant_id

--2. Средний чек
SELECT
    ROUND(CAST(SUM(purchase_total) AS NUMERIC) / CAST(COUNT(DISTINCT purchase_id) AS NUMERIC), 2) AS avg_purchase
FROM
    customer_purchases

--3. Среднее количество товаров в одном заказе
WITH cte2 AS (
  SELECT
    r.purchase_id
,   SUM(r.reciept_rows) AS total_rr
  FROM
    receipts AS r
  GROUP BY
    r.purchase_id
)
SELECT
  ROUND(CAST(AVG(total_rr) AS NUMERIC), 2) AS avg_qrr
FROM cte2;

--4. Retention Rate по когортам
SELECT
    DATE_TRUNC('MONTH', pr.joined_at) AS cohort_month
,   COUNT(DISTINCT pr.plf_participant_id) AS total_cohort_users
,   COUNT(DISTINCT CASE
        WHEN (cp.purchase_at >= pr.joined_at AND cp.purchase_at < pr.joined_at + INTERVAL '1 month')
        THEN cp.customer_id
    END) AS active_users_first_month
,   ROUND(CAST(
        COUNT(DISTINCT CASE
            WHEN (cp.purchase_at >= pr.joined_at AND cp.purchase_at < pr.joined_at + INTERVAL '1 month')
            THEN cp.customer_id
        END) AS NUMERIC)
        / NULLIF(COUNT(DISTINCT pr.plf_participant_id), 0)
,   4) AS retention_rate
FROM
	plf_participants AS pr
LEFT JOIN
	customer_purchases AS cp ON pr.plf_participant_id = cp.customer_id
GROUP BY
	DATE_TRUNC('MONTH', pr.joined_at)
ORDER BY
	DATE_TRUNC('MONTH', pr.joined_at)
;

--5. LTV на пользователя по когортам
WITH cohort_users AS (
    SELECT
        DATE_TRUNC('month', joined_at) AS cohort_month
,       COUNT(DISTINCT plf_participant_id) AS total_users
    FROM 
        plf_participants
    GROUP BY 
        DATE_TRUNC('month', joined_at)
),
cohort_revenue AS (
    SELECT
        DATE_TRUNC('month', pp.joined_at) AS cohort_month
,       SUM(CAST(cp.purchase_total AS NUMERIC) AS revenue_first_3_months
    FROM 
        plf_participants AS pp
    INNER JOIN 
        customer_purchases AS cp ON pp.plf_participant_id = cp.customer_id
    WHERE
        cp.purchase_at >= pp.joined_at AND cp.purchase_at < pp.joined_at + INTERVAL '3 months'
    GROUP BY DATE_TRUNC('month', pp.joined_at)
)
SELECT
    cu.cohort_month
,   cu.total_users
,   COALESCE(cr.revenue_first_3_months, 0) AS three_month_revenue
,   ROUND(
        COALESCE(cr.revenue_first_3_months, 0) / NULLIF(cu.total_users, 0)
,   2) AS ltv_three_month
FROM 
    cohort_users AS cu
LEFT JOIN 
    cohort_revenue AS cr USING(cohort_month)
ORDER BY 
    cu.cohort_month
;

--6. ARPU
WITH cte3 AS (
    SELECT
        SUM(purchase_total) AS sum_rev
    FROM
        customer_purchases
    WHERE
        purchase_at BETWEEN '2025-01-01' AND '2025-05-31' AND purchase_state != 'canceled'
),
registered_users AS (
    SELECT
        COUNT(DISTINCT plf_participant_id) AS all_users
    FROM
        plf_participants
    WHERE
        joined_at <= '2025-05-31'
)
SELECT
    ROUND((SELECT sum_rev FROM cte3) / NULLIF((SELECT all_users FROM registered_users), 0)) AS ARPU
;

--7. ARPPU
WITH paying_users AS (
    SELECT 
        DISTINCT customer_id
    FROM
        customer_purchases
    WHERE
        purchase_at BETWEEN '2025-05-01' AND '2025-05-31' AND purchase_state != 'canceled'
),
revenue AS (
    SELECT
        CAST(SUM(purchase_total) AS NUMERIC) AS total_revenue
    FROM
        customer_purchases
    WHERE
        purchase_at BETWEEN '2025-05-01' AND '2025-05-31' AND purchase_state != 'canceled'
)
SELECT
    r.total_revenue
,   COUNT(p.customer_id) AS paying_users_count
,   ROUND(
        CAST(r.total_revenue AS NUMERIC) / CAST(COUNT(p.customer_id) AS NUMERIC)
    , 2) AS ARPPU
FROM 
    revenue AS r
,   paying_users AS p
GROUP BY
    r.total_revenue
;

--8. DAU
SELECT
    DATE(DATE_TRUNC('DAY', cp.purchase_at)) AS or_d
,   COUNT(DISTINCT pp.plf_participant_id)
FROM 
    plf_participants AS pp
INNER JOIN 
    customer_purchases AS cp
ON 
    pp.plf_participant_id = cp.customer_id
WHERE
    cp.purchase_at BETWEEN '2025-05-01' AND '2025-05-31'
GROUP BY
    DATE_TRUNC('DAY', cp.purchase_at)
ORDER BY
    or_d
;