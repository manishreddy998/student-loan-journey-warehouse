WITH ranked AS (
    SELECT
        applicant_id,
        origin_country,
        origin_country_code,
        kyc_status,
        study_permit_status,
        risk_tier,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        CAST(cdc_lsn AS BIGINT) AS cdc_lsn,
        ROW_NUMBER() OVER (PARTITION BY applicant_id ORDER BY cdc_lsn DESC) AS rn
    FROM {{ source('raw', 'cdc_applicants_history') }}
)
SELECT
    applicant_id,
    origin_country,
    origin_country_code,
    kyc_status,
    study_permit_status,
    risk_tier,
    updated_at,
    cdc_lsn
FROM ranked
WHERE rn = 1
