WITH ranked AS (
    SELECT
        loan_id,
        applicant_id,
        application_id,
        active_guarantor_id,
        loan_status,
        CAST(approved_principal_cad AS DOUBLE) AS approved_principal_cad,
        CAST(tuition_component_cad AS DOUBLE) AS tuition_component_cad,
        CAST(stipend_component_cad AS DOUBLE) AS stipend_component_cad,
        CAST(disbursed_total_cad AS DOUBLE) AS disbursed_total_cad,
        CAST(interest_rate_pct AS DOUBLE) AS interest_rate_pct,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        CAST(cdc_lsn AS BIGINT) AS cdc_lsn,
        ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY cdc_lsn DESC) AS rn
    FROM {{ source('raw', 'cdc_loan_contracts_history') }}
)
SELECT
    loan_id,
    applicant_id,
    application_id,
    active_guarantor_id,
    loan_status,
    approved_principal_cad,
    tuition_component_cad,
    stipend_component_cad,
    disbursed_total_cad,
    interest_rate_pct,
    updated_at,
    cdc_lsn
FROM ranked
WHERE rn = 1
