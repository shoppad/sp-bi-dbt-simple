WITH
    user_matching AS (SELECT * FROM {{ ref("stg_anonymous_to_known_user_matching") }}),

    session_starts AS (
        SELECT
            user_pseudo_id,
            page_location,
            event_timestamp_pt,

            -- URL parts
            CASE WHEN page_location ILIKE 'http%'
                THEN PARSE_URL(page_location)
                ELSE NULL
                END AS parsed_url,
             parsed_url:host || '/' || parsed_url:path AS page_url,
            parsed_url:host::STRING AS page_host,
            '/' || parsed_url:path::STRING AS page_path,
            '?' || parsed_url:query::STRING AS page_query,

            -- Attribution
            COALESCE(traffic_source_name, param_campaign) AS utm_campaign,
            COALESCE(traffic_source_medium, param_medium) AS utm_medium,
            COALESCE(traffic_source_source, param_source) AS utm_source,
            device_category,
            param_content AS utm_content,
            param_term AS utm_term,
            * ILIKE 'referrer%',

            -- App Store
            * ILIKE 'app_store%'

        FROM {{ ref("stg_ga4_events") }}
        WHERE event_name = 'session_start'
    )

SELECT * EXCLUDE (parsed_url)
FROM session_starts
INNER JOIN user_matching USING (user_pseudo_id)
