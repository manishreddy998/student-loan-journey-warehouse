WITH manual_lineage AS (
    SELECT
        'LINEAGE-' || q.applicant_id AS work_item_id,
        q.applicant_id,
        q.followup_application_id AS entity_id,
        'ambiguous_deferral_lineage' AS queue_type,
        'high' AS priority,
        q.review_reason AS reason_summary,
        CAST(q.days_between_submissions AS INTEGER) AS metric_value
    FROM core.lineage_review_queue q
),
guarantor_replacements AS (
    SELECT
        'GUAR-' || applicant_id AS work_item_id,
        applicant_id,
        active_guarantor_id AS entity_id,
        'guarantor_replacement_audit' AS queue_type,
        CASE WHEN guarantor_replacement_hops >= 2 THEN 'high' ELSE 'medium' END AS priority,
        'Applicant replaced co-signer across ' || CAST(total_guarantors_for_applicant AS VARCHAR) || ' guarantor submissions' AS reason_summary,
        CAST(guarantor_replacement_hops AS INTEGER) AS metric_value
    FROM {{ ref('fct_applicant_milestones') }}
    WHERE guarantor_replacement_hops > 0
),
fee_reversals AS (
    SELECT
        'FEE-' || applicant_id AS work_item_id,
        applicant_id,
        loan_id AS entity_id,
        'fee_reversal_reconciliation' AS queue_type,
        'medium' AS priority,
        'Application processing fee reversed or refunded; verified net fee ledger balance' AS reason_summary,
        CAST(reversed_fee_amount_cad AS INTEGER) AS metric_value
    FROM {{ ref('fct_applicant_milestones') }}
    WHERE had_fee_reversal = TRUE
)
SELECT * FROM manual_lineage
UNION ALL
SELECT * FROM guarantor_replacements
UNION ALL
SELECT * FROM fee_reversals
