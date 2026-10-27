SELECT
    channel_name,
    utm_source,
    utm_medium,
    COUNT(DISTINCT click_id) AS deduplicated_clicks,
    COUNT(DISTINCT applicant_id) AS touched_applicants,
    ROUND(SUM(ad_spend_cad), 2) AS total_ad_spend_cad,
    ROUND(SUM(CASE WHEN is_funded_loan THEN first_touch_weight ELSE 0.0 END), 2) AS first_touch_funded_loans,
    ROUND(SUM(CASE WHEN is_funded_loan THEN last_touch_weight ELSE 0.0 END), 2) AS last_touch_funded_loans,
    ROUND(SUM(CASE WHEN is_funded_loan THEN position_based_weight ELSE 0.0 END), 2) AS position_based_funded_loans,
    ROUND(SUM(first_touch_attributed_loan_cad), 2) AS first_touch_disbursed_cad,
    ROUND(SUM(last_touch_attributed_loan_cad), 2) AS last_touch_disbursed_cad,
    ROUND(SUM(position_based_attributed_loan_cad), 2) AS position_based_disbursed_cad,
    CASE
        WHEN SUM(CASE WHEN is_funded_loan THEN position_based_weight ELSE 0.0 END) > 0
        THEN ROUND(SUM(ad_spend_cad) / SUM(CASE WHEN is_funded_loan THEN position_based_weight ELSE 0.0 END), 2)
        ELSE 0.0
    END AS position_based_cac_cad,
    CASE
        WHEN SUM(ad_spend_cad) > 0
        THEN ROUND(SUM(position_based_attributed_loan_cad) / SUM(ad_spend_cad), 2)
        ELSE 0.0
    END AS position_based_roas_x
FROM {{ ref('int_ad_click_attribution') }}
GROUP BY channel_name, utm_source, utm_medium
