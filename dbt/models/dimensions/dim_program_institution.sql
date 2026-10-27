SELECT
    institution_id,
    dli_number,
    institution_name,
    province_code,
    province_name,
    city,
    program_name,
    program_category,
    CAST(annual_tuition_cad AS DOUBLE) AS annual_tuition_cad
FROM {{ source('raw', 'seed_institutions') }}
