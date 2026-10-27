WITH ordered AS (
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
        CAST(updated_at AS TIMESTAMP) AS valid_from,
        CAST(cdc_lsn AS BIGINT) AS cdc_lsn,
        ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY cdc_lsn ASC) AS version_num,
        LEAD(CAST(updated_at AS TIMESTAMP)) OVER (PARTITION BY loan_id ORDER BY cdc_lsn ASC) AS next_valid_from
    FROM {{ source('raw', 'cdc_loan_contracts_history') }}
)
SELECT
    loan_id || '-V' || CAST(version_num AS VARCHAR) AS loan_version_sk,
    loan_id,
    applicant_id,
    application_id,
    active_guarantor_id,
    version_num,
    loan_status,
    approved_principal_cad,
    tuition_component_cad,
    stipend_component_cad,
    disbursed_total_cad,
    interest_rate_pct,
    valid_from,
    COALESCE(next_valid_from, CAST('9999-12-31 23:59:59' AS TIMESTAMP)) AS valid_to,
    CASE WHEN next_valid_from IS NULL THEN TRUE ELSE FALSE END AS is_current,
    CASE
        WHEN next_valid_from IS NOT NULL
        THEN ROUND(DATE_DIFF('hour', valid_from, next_valid_from), 1)
        ELSE 0.0
    END AS hours_in_stage,
    cdc_lsn
FROM ordered
