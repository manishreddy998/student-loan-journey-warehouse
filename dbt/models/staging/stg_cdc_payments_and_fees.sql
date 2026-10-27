SELECT
    payment_id,
    applicant_id,
    loan_id,
    payment_type,
    direction,
    CAST(gross_amount_cad AS DOUBLE) AS gross_amount_cad,
    CAST(fx_fee_cad AS DOUBLE) AS fx_fee_cad,
    payment_status,
    NULLIF(TRIM(COALESCE(reverses_payment_id, '')), '') AS reverses_payment_id,
    CAST(settled_at AS TIMESTAMP) AS settled_at,
    CAST(cdc_lsn AS BIGINT) AS cdc_lsn
FROM {{ source('raw', 'cdc_payments_and_fees_latest') }}
WHERE COALESCE(is_deleted_tombstone, FALSE) = FALSE
