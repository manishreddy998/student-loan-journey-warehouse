{{ config(tags=['serving']) }}
-- Singular test: ensure fct_applicant_milestones has exactly 1 row per applicant
-- and zero superseded deferral rows leak into active milestones
WITH counts AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT applicant_id) AS distinct_applicants
    FROM {{ ref('fct_applicant_milestones') }}
),
superseded_leak AS (
    SELECT COUNT(*) AS leaked_rows
    FROM {{ ref('fct_applicant_milestones') }} m
    INNER JOIN {{ ref('int_deferral_lineage_chains') }} d
        ON m.active_application_id = d.application_id
    WHERE d.is_superseded_by_deferral = TRUE
)
SELECT *
FROM counts, superseded_leak
WHERE total_rows != distinct_applicants
   OR leaked_rows > 0
