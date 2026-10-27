WITH ordered_touches AS (
    SELECT
        c.click_id,
        c.click_event_id,
        c.browser_anon_id,
        c.applicant_id,
        c.stitch_method,
        c.channel_name,
        c.utm_source,
        c.utm_medium,
        c.utm_campaign,
        c.ad_spend_cad,
        c.click_ts,
        ROW_NUMBER() OVER (PARTITION BY c.applicant_id ORDER BY c.click_ts ASC, c.click_id ASC) AS touch_rank_asc,
        ROW_NUMBER() OVER (PARTITION BY c.applicant_id ORDER BY c.click_ts DESC, c.click_id DESC) AS touch_rank_desc,
        COUNT(*) OVER (PARTITION BY c.applicant_id) AS total_touches_for_applicant
    FROM {{ ref('stg_ad_clicks_and_utms') }} c
),
weighted AS (
    SELECT
        t.*,
        CASE WHEN t.touch_rank_asc = 1 THEN 1.0 ELSE 0.0 END AS first_touch_weight,
        CASE WHEN t.touch_rank_desc = 1 THEN 1.0 ELSE 0.0 END AS last_touch_weight,
        CASE
            WHEN t.total_touches_for_applicant = 1 THEN 1.0
            WHEN t.total_touches_for_applicant = 2 THEN 0.5
            WHEN t.touch_rank_asc = 1 OR t.touch_rank_desc = 1 THEN 0.4
            ELSE 0.2 / CAST(t.total_touches_for_applicant - 2 AS DOUBLE)
        END AS position_based_weight,
        l.loan_id,
        l.loan_status,
        CASE WHEN l.loan_status = 'disbursed_active' THEN TRUE ELSE FALSE END AS is_funded_loan,
        l.approved_principal_cad,
        l.disbursed_total_cad
    FROM ordered_touches t
    LEFT JOIN {{ ref('stg_cdc_loan_contracts') }} l
        ON t.applicant_id = l.applicant_id
)
SELECT
    *,
    ROUND(first_touch_weight * COALESCE(disbursed_total_cad, 0.0), 2) AS first_touch_attributed_loan_cad,
    ROUND(last_touch_weight * COALESCE(disbursed_total_cad, 0.0), 2) AS last_touch_attributed_loan_cad,
    ROUND(position_based_weight * COALESCE(disbursed_total_cad, 0.0), 2) AS position_based_attributed_loan_cad
FROM weighted
