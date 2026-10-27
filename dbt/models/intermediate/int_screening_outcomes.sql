SELECT
    applicant_id,
    application_id,
    COUNT(*) AS total_checks_run,
    MAX(CASE WHEN check_type = 'academic_eligibility' THEN screening_outcome END) AS academic_check_outcome,
    MAX(CASE WHEN check_type = 'document_authenticity' THEN screening_outcome END) AS document_check_outcome,
    MAX(CASE WHEN check_type = 'financial_capacity' THEN screening_outcome END) AS financial_check_outcome,
    CASE
        WHEN SUM(CASE WHEN screening_outcome = 'passed' THEN 1 ELSE 0 END) = COUNT(*) THEN TRUE
        ELSE FALSE
    END AS all_screenings_passed,
    ROUND(AVG(screening_score), 1) AS avg_screening_score,
    MAX(evaluated_at) AS screening_completed_at
FROM {{ ref('stg_cdc_screening_checks') }}
GROUP BY applicant_id, application_id
