/*
Датасет для анализа пользователей
*/
WITH cstmr_purchases AS (
    SELECT
        cp.purchase_id
,       cp.purchase_at
,       cp.purchase_state
,       cp.customer_id
,       cp.purchase_total
    FROM
        customer_purchases AS cp
    WHERE
        cp.purchase_at BETWEEN '01-06-2024' AND '31-05-2025'
), 
plf_ps AS (
    SELECT
        pp.plf_participant_id
,       pp.fn_ln_p
,       pp.contact_mail
,       pp.joined_at
,       pp.role
,       pp.activity
    FROM
        plf_participants AS pp
    WHERE
        pp.joined_at <= '31-05-2025'
), 
cte AS (
    SELECT
        pp.plf_participant_id
,       pp.fn_ln_p
,       pp.contact_mail
,       pp.joined_at
,       pp.role
,       pp.activity
,       cp.purchase_id
,       cp.purchase_at
,       cp.purchase_total
,       cp.purchase_state
    FROM
        plf_ps AS pp
    LEFT JOIN 
        cstmr_purchases AS cp ON cp.customer_id = pp.plf_participant_id)
SELECT
    plf_participant_id
,   fn_ln_p
,   contact_mail
,   role
,   joined_at
,   activity
,   purchase_id
,   purchase_at
,   purchase_state
,   purchase_total
FROM
    cte
;