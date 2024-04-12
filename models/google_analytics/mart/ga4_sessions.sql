WITH
session_starts AS (
    SELECT
        *
    FROM {{ ref('int_ga4_events') }}
    WHERE event_name = 'session_start'
),

pageview_events AS (
    SELECT
        *
    FROM {{ ref("int_ga4_events") }}
    WHERE event_name = 'page_view'
),

page_counts AS (
    SELECT
         ga_session_id,
         ga_session_number,
         MAX(shop_subdomain) AS shop_subdomain,
         COUNT(*) AS page_view_count
    FROM pageview_events
    GROUP BY 1, 2
),

landing_pages AS (
    SELECT
        * EXCLUDE (
          event_name,
          event_id,
          event_order,
          is_landing_pageview,
          is_exit_pageview
        ) RENAME (
            event_timestamp_pt AS landing_page_timestamp,
            page_title AS landing_page_title,
            page_location_page_type AS landing_page_type,
            page_location_path AS landing_page_path,
            page_location_url AS landing_page_url,
            page_location_query AS landing_page_query,
            page_location_host AS landing_page_host,
            page_referrer_host AS session_referrer_host,
            page_referrer_url AS session_referrer_url,
            page_referrer_path AS session_referrer_path,
            page_referrer_query AS session_referrer_query,
            page_referrer_full AS session_referrer_full
        )

    FROM session_starts
),

exit_pages AS (
    SELECT
        ga_session_id,
        event_timestamp_pt AS exit_page_timestamp,
        page_title AS exit_page_title,
        page_location_page_type AS exit_page_type,
        page_location_path AS exit_page_path,
        page_location_url AS exit_page_url,
        page_location_query AS exit_page_search,
        page_location_host AS exit_page_host
    FROM pageview_events
    WHERE is_exit_pageview
),

final AS (

    SELECT
        *
    FROM landing_pages
    LEFT JOIN page_counts
        USING (ga_session_id)
    LEFT JOIN exit_pages
        USING (ga_session_id)
)

SELECT *
FROM final
