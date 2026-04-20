/*
Датасет для анализа продаж
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
main AS (
    SELECT
        cp.purchase_id
,       cp.purchase_at
,       cp.purchase_state
,       cp.purchase_total AS order_purchase_total
,       r.receipt_id
,       r.reciept_rows
,       r.reciept_at
,       i.item_id
,       i.item_name
,       i.item_parameters
,       i.item_cost
,       i.item_count
,       c.class_id
,       c.class_name
,       bpp.plf_participant_id AS buyer_plf_participant_id
,       bpp.fn_ln_p AS buyer_fn_ln_p
,       bpp.contact_mail AS buyer_contact_mail
,       bpp.joined_at AS buyer_joined_at
,       bpp.activity AS buyer_activity
,       spp.plf_participant_id AS seller_plf_participant_id
,       spp.fn_ln_p AS seller_fn_ln_p
,       spp.contact_mail AS seller_contact_mail
,       spp.joined_at AS seller_joined_at
,       spp.activity AS seller_activity
,       f.feedback_id
,       f.item_score
,       f.feedback_text
,       f.feedback_at
    FROM 
        cstmr_purchases AS cp
    LEFT JOIN
        plf_participants AS bpp ON cp.customer_id = bpp.plf_participant_id
    LEFT JOIN
        receipts AS r ON cp.purchase_id = r.purchase_id
    LEFT JOIN
        items AS i ON r.item_id = i.item_id
    LEFT JOIN
        plf_participants AS spp ON i.seller_id = spp.plf_participant_id
    LEFT JOIN
        classes AS c ON i.class_id = c.class_id
    LEFT JOIN
        feedback AS f on f.plf_participant_id = bpp.plf_participant_id and f.item_id = i.item_id
)
SELECT
    purchase_id
,   purchase_at
,   purchase_state
,   order_purchase_total
,   receipt_id
,   reciept_rows
,   reciept_at
,   item_id
,   item_parameters
,   item_name
,   item_cost
,   item_count
,   class_id
,   class_name
,   buyer_plf_participant_id
,   buyer_fn_ln_p
,   buyer_contact_mail
,   buyer_joined_at
,   buyer_activity
,   seller_plf_participant_id
,   seller_fn_ln_p
,   seller_contact_mail
,   seller_joined_at
,   seller_activity
,   feedback_id
,   item_score
,   feedback_text
,   feedback_at
,   CASE
        WHEN purchase_id IS NULL
            THEN NULL
        ELSE ROW_NUMBER() OVER (PARTITION BY purchase_id)
    END AS rn_purchase_id
,   CASE
        WHEN feedback_id IS NULL
            THEN NULL
        ELSE ROW_NUMBER() OVER (PARTITION BY feedback_id)
    END AS rn_feedback_id
,   SUM(reciept_rows) OVER (PARTITION BY purchase_id) AS receipts_per_purchase
,   AVG(item_score) OVER (PARTITION BY item_id) AS avg_item_score
FROM
    main
;