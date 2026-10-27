WITH reversal_links AS (
    SELECT DISTINCT reverses_payment_id AS reversed_payment_id
    FROM {{ ref('stg_cdc_payments_and_fees') }}
    WHERE reverses_payment_id IS NOT NULL
),
enriched_ledger AS (
    SELECT
        p.payment_id,
        p.applicant_id,
        p.loan_id,
        p.payment_type,
        p.direction,
        p.gross_amount_cad,
        p.fx_fee_cad,
        p.payment_status,
        p.reverses_payment_id,
        p.settled_at,
        p.cdc_lsn,
        CASE
            WHEN r.reversed_payment_id IS NOT NULL OR p.reverses_payment_id IS NOT NULL OR p.payment_status = 'reversed'
            THEN TRUE
            ELSE FALSE
        END AS is_reversed_or_refund_entry,
        CASE
            WHEN p.direction IN ('inbound_fee', 'inbound_fee_reversal') THEN p.gross_amount_cad
            ELSE 0.0
        END AS signed_fee_revenue_cad,
        CASE
            WHEN p.direction = 'outbound_disbursement' AND p.payment_status = 'settled' THEN p.gross_amount_cad
            ELSE 0.0
        END AS settled_disbursement_cad
    FROM {{ ref('stg_cdc_payments_and_fees') }} p
    LEFT JOIN reversal_links r
        ON p.payment_id = r.reversed_payment_id
)
SELECT *
FROM enriched_ledger
