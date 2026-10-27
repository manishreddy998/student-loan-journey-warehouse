SELECT
    screening_id,
    applicant_id,
    application_id,
    check_type,
    screening_outcome,
    CAST(score AS INTEGER) AS screening_score,
    CAST(evaluated_at AS TIMESTAMP) AS evaluated_at,
    CAST(cdc_lsn AS BIGINT) AS cdc_lsn
FROM {{ source('raw', 'cdc_screening_checks_latest') }}
WHERE COALESCE(is_deleted_tombstone, FALSE) = FALSE
