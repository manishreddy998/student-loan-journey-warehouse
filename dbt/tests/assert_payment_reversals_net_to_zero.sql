{{ config(tags=['serving']) }}
-- Singular test: every reversed payment paired with its reversal entry must net to 0.00 CAD
WITH reversals AS (
    SELECT
        rev.payment_id AS reversal_id,
        orig.payment_id AS original_id,
        ROUND(orig.gross_amount_cad + rev.gross_amount_cad, 2) AS net_pair_cad
    FROM {{ ref('fct_payment_fee_ledger') }} rev
    INNER JOIN {{ ref('fct_payment_fee_ledger') }} orig
        ON rev.reverses_payment_id = orig.payment_id
)
SELECT *
FROM reversals
WHERE ABS(net_pair_cad) > 0.001
