SELECT
    loan_version_sk,
    loan_id,
    applicant_id,
    application_id,
    active_guarantor_id,
    version_num AS stage_sequence_num,
    loan_status,
    approved_principal_cad,
    tuition_component_cad,
    stipend_component_cad,
    disbursed_total_cad,
    interest_rate_pct,
    valid_from,
    valid_to,
    is_current,
    hours_in_stage,
    cdc_lsn
FROM {{ ref('dim_loan_status_scd2') }}
