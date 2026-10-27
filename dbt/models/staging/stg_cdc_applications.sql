SELECT
    application_id,
    applicant_id,
    institution_id,
    program_name,
    intake_cohort,
    NULLIF(TRIM(COALESCE(parent_application_id, '')), '') AS stated_parent_application_id,
    NULLIF(TRIM(COALESCE(resolved_parent_application_id, '')), '') AS resolved_parent_application_id,
    lineage_link_method,
    application_status,
    CAST(requested_loan_cad AS DOUBLE) AS requested_loan_cad,
    CAST(submitted_at AS TIMESTAMP) AS submitted_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(cdc_lsn AS BIGINT) AS cdc_lsn
FROM {{ source('raw', 'cdc_applications_latest') }}
WHERE COALESCE(is_deleted_tombstone, FALSE) = FALSE
