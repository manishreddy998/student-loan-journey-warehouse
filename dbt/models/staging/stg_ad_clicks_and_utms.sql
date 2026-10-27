WITH dedup_clicks AS (
    SELECT
        click_event_id,
        click_id,
        browser_anon_id,
        NULLIF(TRIM(COALESCE(applicant_id, '')), '') AS direct_applicant_id,
        channel_name,
        utm_source,
        utm_medium,
        utm_campaign,
        CAST(ad_spend_cad AS DOUBLE) AS ad_spend_cad,
        CAST(click_ts AS TIMESTAMP) AS click_ts,
        ROW_NUMBER() OVER (PARTITION BY click_id ORDER BY click_ts ASC, click_event_id ASC) AS click_rn
    FROM {{ source('raw', 'ad_click_events') }}
),
stitched AS (
    SELECT
        c.click_event_id,
        c.click_id,
        c.browser_anon_id,
        COALESCE(c.direct_applicant_id, m.applicant_id) AS applicant_id,
        COALESCE(m.stitch_method, 'direct_authenticated') AS stitch_method,
        c.channel_name,
        c.utm_source,
        c.utm_medium,
        c.utm_campaign,
        c.ad_spend_cad,
        c.click_ts
    FROM dedup_clicks c
    LEFT JOIN {{ source('raw', 'identity_stitch_map') }} m
        ON c.browser_anon_id = m.browser_anon_id
    WHERE c.click_rn = 1
)
SELECT *
FROM stitched
