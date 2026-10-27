{{ config(tags=['serving']) }}
-- Singular test: ensure every applicant has exactly 1 active guarantor and zero tombstoned guarantors are active
WITH per_app AS (
    SELECT
        applicant_id,
        SUM(CASE WHEN is_active_guarantor THEN 1 ELSE 0 END) AS active_cnt,
        SUM(CASE WHEN is_active_guarantor AND is_deleted_tombstone THEN 1 ELSE 0 END) AS tombstone_active_cnt
    FROM {{ ref('int_guarantor_replacement_chains') }}
    GROUP BY applicant_id
)
SELECT *
FROM per_app
WHERE active_cnt != 1
   OR tombstone_active_cnt > 0
