WITH
    first_install_events AS (
        SELECT
            * RENAME (
                getmesa_install_convert_event_timestamp_pt AS first_install_timestamp_pt
            )
        FROM {{ ref("stg_ga_install_events") }}
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY shop_subdomain
                ORDER BY getmesa_install_convert_event_timestamp_pt
            )
            = 1
    ),

    session_starts AS (SELECT * FROM {{ ref("stg_ga_session_starts") }}),

    last_touch_sessions AS (
        SELECT * EXCLUDE (rn)
        FROM
            (
                SELECT
                    session_starts.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY first_install_events.shop_subdomain
                        ORDER BY session_starts.event_timestamp_pt DESC
                    ) AS rn
                FROM session_starts
                INNER JOIN first_install_events USING (shop_subdomain)
                WHERE
                    session_starts.event_timestamp_pt
                    <= first_install_events.first_install_timestamp_pt
                QUALIFY rn = 1
            ) AS t
    ),

    formatted_last_touch_session_starts AS (
        SELECT
            shop_subdomain,
            event_timestamp_pt AS last_touch_at_pt,
            utm_content AS last_touch_content,
            utm_campaign AS last_touch_campaign,
            utm_medium AS last_touch_medium,
            utm_source AS last_touch_source,
            app_store_surface_detail AS last_touch_app_store_surface_detail,
            app_store_surface_type AS last_touch_app_store_surface_type,
            app_store_surface_intra_position
                AS last_touch_app_store_surface_intra_position,
            app_store_locale AS last_touch_app_store_locale,
            app_store_surface_inter_position
                AS last_touch_app_store_surface_inter_position,
            page_referrer AS last_touch_referrer,
            PARSE_URL(last_touch_referrer):host::STRING AS last_touch_referrer_host,
            device_category AS last_touch_device_category,

            PARSE_URL(page_location) AS parsed_url,
            parsed_url:host || '/' || parsed_url:path AS last_touch_url,
            parsed_url:host::STRING AS last_touch_host,
            '/' || parsed_url:path::STRING AS last_touch_path,
            '?' || parsed_url:query::STRING AS last_touch_query

        FROM last_touch_sessions
    )

SELECT *
FROM formatted_last_touch_session_starts
