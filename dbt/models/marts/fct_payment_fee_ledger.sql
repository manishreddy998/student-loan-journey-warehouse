SELECT
    payment_id,
    applicant_id,
    loan_id,
    payment_type,
    direction,
    gross_amount_cad,
    fx_fee_cad,
    payment_status,
    reverses_payment_id,
    is_reversed_or_refund_entry,
    signed_fee_revenue_cad,
    settled_disbursement_cad,
    settled_at,
    cdc_lsn
FROM {{ ref('int_payment_fee_reconciliation') }}
