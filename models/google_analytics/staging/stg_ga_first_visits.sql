WITH
    shop_anonymous_keys AS (
        SELECT * FROM {{ ref("stg_anonymous_to_known_user_matching") }}
    ),

    first_visits AS (
        SELECT *
        FROM {{ ref("stg_ga4_events") }}
        WHERE (event_name = 'first_visit') AND NOT (page_location ILIKE '%.pages.dev%')
    ),

    shop_first_visits AS (

        SELECT
            {# App Store #}
            * ILIKE 'app_store%',

            {# Identifiers #}
            user_pseudo_id::STRING AS user_pseudo_id,
            page_location::STRING AS page_location,
            event_timestamp_pt,

            {# Attribution #}
            param_content AS utm_content,
            param_term AS utm_term,
            COALESCE(traffic_source_name, param_campaign) AS utm_campaign,
            COALESCE(traffic_source_medium, param_medium) AS utm_medium,
            COALESCE(traffic_source_source, param_source) AS utm_source,
            device_category,
            page_referrer AS first_touch_referrer,
            parse_url(first_touch_referrer):host::STRING AS first_touch_referrer_host

        FROM first_visits
    ),

    formatted_first_visits AS (
        SELECT
            user_pseudo_id,
            event_timestamp_pt AS first_touch_at_pt,
            page_location AS acquisition_first_page_path,
            utm_content AS first_touch_content,
            utm_campaign AS first_touch_campaign,
            utm_medium AS first_touch_medium,
            utm_source AS first_touch_source,
            app_store_surface_detail AS first_touch_app_surface_detail,
            app_store_surface_type AS first_touch_app_store_surface_type,
            app_store_surface_intra_position
                AS first_touch_app_store_surface_intra_position,
            app_store_surface_inter_position
                AS first_touch_app_store_surface_inter_position,
            app_store_locale AS first_touch_app_store_locale,
            first_touch_referrer,
            first_touch_referrer_host,
            device_category AS first_touch_device_category,

            parse_url(page_location) AS parsed_url,
            parsed_url:host || '/' || parsed_url:path AS first_touch_url,
            parsed_url:host::STRING AS first_touch_host,
            '/' || parsed_url:path::STRING AS first_touch_path,
            '?' || parsed_url:query::STRING AS first_touch_query
        FROM shop_first_visits
    )

SELECT * EXCLUDE (parsed_url)
FROM formatted_first_visits
INNER JOIN shop_anonymous_keys USING (user_pseudo_id)
QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY first_touch_at_pt) = 1
