SELECT
    guarantor_id,
    applicant_id,
    CAST(guarantor_seq AS INTEGER) AS guarantor_seq,
    NULLIF(TRIM(COALESCE(replaced_by_guarantor_id, '')), '') AS replaced_by_guarantor_id,
    relationship,
    credit_score_band,
    CAST(annual_income_cad AS DOUBLE) AS annual_income_cad,
    guarantor_status,
    COALESCE(is_deleted_tombstone, FALSE) AS is_deleted_tombstone,
    CAST(verified_at AS TIMESTAMP) AS verified_at,
    CAST(cdc_lsn AS BIGINT) AS cdc_lsn
FROM {{ source('raw', 'cdc_guarantors_latest') }}
