{{ config(tags=['serving']) }}
-- Singular test: First-Touch, Last-Touch, and Position-Based weights must each sum to 1.0 per applicant
SELECT
    applicant_id,
    ROUND(SUM(first_touch_weight), 4) AS ft_sum,
    ROUND(SUM(last_touch_weight), 4) AS lt_sum,
    ROUND(SUM(position_based_weight), 4) AS pb_sum
FROM {{ ref('int_ad_click_attribution') }}
GROUP BY applicant_id
HAVING ABS(SUM(first_touch_weight) - 1.0) > 0.001
    OR ABS(SUM(last_touch_weight) - 1.0) > 0.001
    OR ABS(SUM(position_based_weight) - 1.0) > 0.001
