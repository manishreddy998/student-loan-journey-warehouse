WITH ordered AS (
    SELECT
        applicant_id,
        origin_country,
        origin_country_code,
        kyc_status,
        study_permit_status,
        risk_tier,
        CAST(updated_at AS TIMESTAMP) AS valid_from,
        CAST(cdc_lsn AS BIGINT) AS cdc_lsn,
        ROW_NUMBER() OVER (PARTITION BY applicant_id ORDER BY cdc_lsn ASC) AS version_num,
        LEAD(CAST(updated_at AS TIMESTAMP)) OVER (PARTITION BY applicant_id ORDER BY cdc_lsn ASC) AS next_valid_from
    FROM {{ source('raw', 'cdc_applicants_history') }}
)
SELECT
    applicant_id || '-V' || CAST(version_num AS VARCHAR) AS applicant_version_sk,
    applicant_id,
    version_num,
    origin_country,
    origin_country_code,
    kyc_status,
    study_permit_status,
    risk_tier,
    valid_from,
    COALESCE(next_valid_from, CAST('9999-12-31 23:59:59' AS TIMESTAMP)) AS valid_to,
    CASE WHEN next_valid_from IS NULL THEN TRUE ELSE FALSE END AS is_current,
    cdc_lsn
FROM ordered
