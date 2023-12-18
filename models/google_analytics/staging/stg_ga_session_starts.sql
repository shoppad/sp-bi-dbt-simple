WITH
    user_matching AS (SELECT * FROM {{ ref("stg_anonymous_to_known_user_matching") }}),

    session_starts AS (
        SELECT
            user_pseudo_id,
            page_location,
            event_timestamp_pt,

            -- URL parts
            SPLIT_PART(page_location, '//', 2) AS page_url,
            SPLIT_PART(page_url, '/', 1) AS page_host,
            SPLIT_PART(page_url, '?', 1) AS page_path,

            -- Attribution
            COALESCE(traffic_source_name, param_campaign) AS utm_campaign,
            COALESCE(traffic_source_medium, param_medium) AS utm_medium,
            COALESCE(traffic_source_source, param_source) AS utm_source,
            device_category,
            param_content AS utm_content,
            param_term AS utm_term,
            page_referrer,
            * ILIKE 'referrer%',

            -- App Store
            * ILIKE 'app_store%'

        FROM {{ ref("stg_ga4_events") }}
        WHERE event_name = 'session_start'
    )

SELECT *
FROM session_starts
INNER JOIN user_matching USING (user_pseudo_id)
