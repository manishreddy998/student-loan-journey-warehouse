WITH deferral_trail AS (
    SELECT
        applicant_id,
        STRING_AGG(
            application_id || ':' || intake_cohort || '(' || application_status || ')',
            ' -> ' ORDER BY deferral_hop_number ASC
        ) AS deferral_lineage_breadcrumb
    FROM {{ ref('int_deferral_lineage_chains') }}
    GROUP BY applicant_id
),
guarantor_trail AS (
    SELECT
        applicant_id,
        STRING_AGG(
            guarantor_id || ':' || relationship || ':' || guarantor_status,
            ' -> ' ORDER BY guarantor_seq ASC
        ) AS guarantor_chain_breadcrumb
    FROM {{ ref('int_guarantor_replacement_chains') }}
    GROUP BY applicant_id
)
SELECT
    m.applicant_id,
    m.origin_country,
    m.origin_country_code,
    m.kyc_status,
    m.study_permit_status,
    m.risk_tier,
    m.root_application_id,
    m.active_application_id,
    m.original_intake_cohort,
    m.active_intake_cohort,
    m.deferral_count,
    m.lineage_link_method,
    m.requires_manual_lineage_review,
    dt.deferral_lineage_breadcrumb,
    m.institution_id,
    m.dli_number,
    m.institution_name,
    m.province_code,
    m.program_name,
    m.program_category,
    m.all_screenings_passed,
    m.avg_screening_score,
    m.initial_guarantor_id,
    m.active_guarantor_id,
    m.guarantor_replacement_hops,
    gt.guarantor_chain_breadcrumb,
    m.active_guarantor_credit_band,
    m.active_guarantor_income_cad,
    m.loan_id,
    m.loan_status,
    m.is_funded,
    m.approved_principal_cad,
    m.disbursed_total_cad,
    m.net_fee_revenue_cad,
    m.had_fee_reversal,
    m.first_touch_channel,
    m.last_touch_channel,
    m.stitched_touch_count
FROM {{ ref('fct_applicant_milestones') }} m
LEFT JOIN deferral_trail dt
    ON m.applicant_id = dt.applicant_id
LEFT JOIN guarantor_trail gt
    ON m.applicant_id = gt.applicant_id
