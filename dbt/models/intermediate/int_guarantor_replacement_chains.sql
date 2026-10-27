WITH ordered_guar AS (
    SELECT
        guarantor_id,
        applicant_id,
        guarantor_seq,
        replaced_by_guarantor_id,
        relationship,
        credit_score_band,
        annual_income_cad,
        guarantor_status,
        is_deleted_tombstone,
        verified_at,
        cdc_lsn,
        ROW_NUMBER() OVER (PARTITION BY applicant_id ORDER BY guarantor_seq DESC) AS rev_rn,
        COUNT(*) OVER (PARTITION BY applicant_id) AS total_guarantors_for_applicant,
        FIRST_VALUE(guarantor_id) OVER (
            PARTITION BY applicant_id
            ORDER BY guarantor_seq ASC
        ) AS initial_guarantor_id,
        FIRST_VALUE(guarantor_id) OVER (
            PARTITION BY applicant_id
            ORDER BY guarantor_seq DESC
        ) AS active_guarantor_id
    FROM {{ ref('stg_cdc_guarantors') }}
)
SELECT
    guarantor_id,
    applicant_id,
    guarantor_seq,
    replaced_by_guarantor_id,
    initial_guarantor_id,
    active_guarantor_id,
    total_guarantors_for_applicant,
    total_guarantors_for_applicant - 1 AS replacement_hops,
    relationship,
    credit_score_band,
    annual_income_cad,
    guarantor_status,
    is_deleted_tombstone,
    CASE
        WHEN rev_rn = 1 AND is_deleted_tombstone = FALSE AND replaced_by_guarantor_id IS NULL THEN TRUE
        ELSE FALSE
    END AS is_active_guarantor,
    CASE
        WHEN rev_rn > 1 OR is_deleted_tombstone = TRUE OR replaced_by_guarantor_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_superseded_guarantor,
    verified_at,
    cdc_lsn
FROM ordered_guar
