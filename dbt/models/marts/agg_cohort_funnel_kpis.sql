SELECT
    active_intake_cohort,
    province_code,
    program_category,
    COUNT(DISTINCT applicant_id) AS total_applicants,
    SUM(CASE WHEN kyc_status = 'kyc_verified' THEN 1 ELSE 0 END) AS kyc_verified_applicants,
    SUM(CASE WHEN all_screenings_passed = TRUE THEN 1 ELSE 0 END) AS screening_passed_applicants,
    SUM(CASE WHEN study_permit_status = 'study_permit_approved' THEN 1 ELSE 0 END) AS study_permit_approved_applicants,
    SUM(CASE WHEN is_funded = TRUE THEN 1 ELSE 0 END) AS funded_loans_count,
    ROUND(100.0 * SUM(CASE WHEN is_funded = TRUE THEN 1 ELSE 0 END) / COUNT(DISTINCT applicant_id), 2) AS funded_conversion_rate_pct,
    SUM(CASE WHEN deferral_count > 0 THEN 1 ELSE 0 END) AS deferred_applicants_count,
    SUM(deferral_count) AS total_deferral_hops,
    SUM(CASE WHEN guarantor_replacement_hops > 0 THEN 1 ELSE 0 END) AS guarantor_replacement_cases,
    ROUND(SUM(approved_principal_cad), 2) AS total_approved_principal_cad,
    ROUND(SUM(disbursed_total_cad), 2) AS total_disbursed_cad,
    ROUND(SUM(net_fee_revenue_cad), 2) AS total_net_fee_revenue_cad
FROM {{ ref('fct_applicant_milestones') }}
GROUP BY active_intake_cohort, province_code, program_category
