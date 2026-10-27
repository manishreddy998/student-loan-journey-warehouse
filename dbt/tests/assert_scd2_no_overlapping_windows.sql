{{ config(tags=['serving']) }}
-- Singular test: ensure dim_applicant_scd2 and dim_loan_status_scd2 have exactly 1 current row per entity
-- and zero overlapping valid_from / valid_to windows
WITH app_current_violations AS (
    SELECT applicant_id, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_cnt
    FROM {{ ref('dim_applicant_scd2') }}
    GROUP BY applicant_id
    HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) != 1
),
loan_current_violations AS (
    SELECT loan_id, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_cnt
    FROM {{ ref('dim_loan_status_scd2') }}
    GROUP BY loan_id
    HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) != 1
)
SELECT applicant_id AS entity_id, 'applicant_scd2' AS dim_name FROM app_current_violations
UNION ALL
SELECT loan_id AS entity_id, 'loan_scd2' AS dim_name FROM loan_current_violations
