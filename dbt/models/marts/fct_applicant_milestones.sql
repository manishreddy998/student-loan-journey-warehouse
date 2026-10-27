WITH active_app AS (
    SELECT *
    FROM {{ ref('int_deferral_lineage_chains') }}
    WHERE is_active_application = TRUE
),
cur_applicant AS (
    SELECT *
    FROM {{ ref('dim_applicant_scd2') }}
    WHERE is_current = TRUE
),
active_guar AS (
    SELECT *
    FROM {{ ref('int_guarantor_replacement_chains') }}
    WHERE is_active_guarantor = TRUE
),
cur_loan AS (
    SELECT *
    FROM {{ ref('dim_loan_status_scd2') }}
    WHERE is_current = TRUE
),
ledger_rollup AS (
    SELECT
        applicant_id,
        ROUND(SUM(CASE WHEN direction = 'inbound_fee' THEN gross_amount_cad ELSE 0.0 END), 2) AS gross_fee_collected_cad,
        ROUND(SUM(CASE WHEN direction = 'inbound_fee_reversal' THEN ABS(gross_amount_cad) ELSE 0.0 END), 2) AS reversed_fee_amount_cad,
        ROUND(SUM(signed_fee_revenue_cad), 2) AS net_fee_revenue_cad,
        ROUND(SUM(fx_fee_cad), 2) AS fx_fee_total_cad,
        ROUND(SUM(CASE WHEN payment_type = 'tuition_disbursement_to_dli' THEN settled_disbursement_cad ELSE 0.0 END), 2) AS tuition_disbursed_cad,
        ROUND(SUM(CASE WHEN payment_type = 'gic_living_stipend_disbursement' THEN settled_disbursement_cad ELSE 0.0 END), 2) AS stipend_disbursed_cad,
        ROUND(SUM(settled_disbursement_cad), 2) AS total_disbursed_cad,
        MAX(CASE WHEN is_reversed_or_refund_entry = TRUE THEN TRUE ELSE FALSE END) AS had_fee_reversal
    FROM {{ ref('int_payment_fee_reconciliation') }}
    GROUP BY applicant_id
),
attr_first_last AS (
    SELECT
        applicant_id,
        MAX(CASE WHEN touch_rank_asc = 1 THEN channel_name END) AS first_touch_channel,
        MAX(CASE WHEN touch_rank_asc = 1 THEN click_id END) AS first_touch_click_id,
        MAX(CASE WHEN touch_rank_desc = 1 THEN channel_name END) AS last_touch_channel,
        MAX(CASE WHEN touch_rank_desc = 1 THEN click_id END) AS last_touch_click_id,
        COUNT(*) AS stitched_touch_count,
        ROUND(SUM(ad_spend_cad), 2) AS total_acquisition_cost_cad
    FROM {{ ref('int_ad_click_attribution') }}
    GROUP BY applicant_id
)
SELECT
    a.applicant_id,
    a.applicant_version_sk,
    a.origin_country,
    a.origin_country_code,
    a.kyc_status,
    a.study_permit_status,
    a.risk_tier,
    ap.root_application_id,
    ap.application_id AS active_application_id,
    ap.original_intake_cohort,
    ap.active_intake_cohort,
    ap.total_deferrals_for_applicant AS deferral_count,
    ap.lineage_link_method,
    ap.requires_manual_lineage_review,
    ap.application_status,
    ap.submitted_at,
    inst.institution_id,
    inst.dli_number,
    inst.institution_name,
    inst.province_code,
    inst.city AS institution_city,
    ap.program_name,
    inst.program_category,
    inst.annual_tuition_cad,
    scr.academic_check_outcome,
    scr.document_check_outcome,
    scr.financial_check_outcome,
    scr.all_screenings_passed,
    scr.avg_screening_score,
    g.initial_guarantor_id,
    g.guarantor_id AS active_guarantor_id,
    g.total_guarantors_for_applicant,
    g.replacement_hops AS guarantor_replacement_hops,
    g.credit_score_band AS active_guarantor_credit_band,
    g.annual_income_cad AS active_guarantor_income_cad,
    g.guarantor_status AS active_guarantor_status,
    ln.loan_id,
    ln.loan_version_sk,
    ln.loan_status,
    CASE WHEN ln.loan_status = 'disbursed_active' THEN TRUE ELSE FALSE END AS is_funded,
    ln.approved_principal_cad,
    ln.tuition_component_cad,
    ln.stipend_component_cad,
    ln.disbursed_total_cad,
    ln.interest_rate_pct,
    ld.gross_fee_collected_cad,
    ld.reversed_fee_amount_cad,
    ld.net_fee_revenue_cad,
    ld.tuition_disbursed_cad,
    ld.stipend_disbursed_cad,
    ld.had_fee_reversal,
    att.first_touch_channel,
    att.first_touch_click_id,
    att.last_touch_channel,
    att.last_touch_click_id,
    att.stitched_touch_count,
    att.total_acquisition_cost_cad
FROM cur_applicant a
INNER JOIN active_app ap
    ON a.applicant_id = ap.applicant_id
INNER JOIN {{ ref('dim_program_institution') }} inst
    ON ap.institution_id = inst.institution_id
INNER JOIN {{ ref('int_screening_outcomes') }} scr
    ON a.applicant_id = scr.applicant_id
INNER JOIN active_guar g
    ON a.applicant_id = g.applicant_id
INNER JOIN cur_loan ln
    ON a.applicant_id = ln.applicant_id
INNER JOIN ledger_rollup ld
    ON a.applicant_id = ld.applicant_id
INNER JOIN attr_first_last att
    ON a.applicant_id = att.applicant_id
