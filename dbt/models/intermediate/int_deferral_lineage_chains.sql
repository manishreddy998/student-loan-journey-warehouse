WITH ordered_apps AS (
    SELECT
        application_id,
        applicant_id,
        institution_id,
        program_name,
        intake_cohort,
        stated_parent_application_id,
        resolved_parent_application_id,
        lineage_link_method,
        application_status,
        requested_loan_cad,
        submitted_at,
        updated_at,
        cdc_lsn,
        ROW_NUMBER() OVER (PARTITION BY applicant_id ORDER BY submitted_at ASC, application_id ASC) - 1 AS deferral_hop_number,
        ROW_NUMBER() OVER (PARTITION BY applicant_id ORDER BY submitted_at DESC, application_id DESC) AS rev_rn,
        COUNT(*) OVER (PARTITION BY applicant_id) - 1 AS total_deferrals_for_applicant,
        FIRST_VALUE(application_id) OVER (
            PARTITION BY applicant_id
            ORDER BY submitted_at ASC, application_id ASC
        ) AS root_application_id,
        FIRST_VALUE(intake_cohort) OVER (
            PARTITION BY applicant_id
            ORDER BY submitted_at ASC, application_id ASC
        ) AS original_intake_cohort,
        FIRST_VALUE(application_id) OVER (
            PARTITION BY applicant_id
            ORDER BY submitted_at DESC, application_id DESC
        ) AS active_application_id,
        FIRST_VALUE(intake_cohort) OVER (
            PARTITION BY applicant_id
            ORDER BY submitted_at DESC, application_id DESC
        ) AS active_intake_cohort
    FROM {{ ref('stg_cdc_applications') }}
)
SELECT
    application_id,
    applicant_id,
    institution_id,
    program_name,
    intake_cohort,
    stated_parent_application_id,
    resolved_parent_application_id,
    lineage_link_method,
    root_application_id,
    original_intake_cohort,
    active_application_id,
    active_intake_cohort,
    deferral_hop_number,
    total_deferrals_for_applicant,
    application_status,
    requested_loan_cad,
    CASE WHEN rev_rn > 1 THEN TRUE ELSE FALSE END AS is_superseded_by_deferral,
    CASE WHEN rev_rn = 1 THEN TRUE ELSE FALSE END AS is_active_application,
    CASE WHEN lineage_link_method = 'routed_to_review_queue' THEN TRUE ELSE FALSE END AS requires_manual_lineage_review,
    submitted_at,
    updated_at,
    cdc_lsn
FROM ordered_apps
