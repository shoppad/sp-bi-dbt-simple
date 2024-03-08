WITH
ga4_events AS (
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
    FROM ga4_events
    GROUP BY 1, 2
),

landing_pages AS (
    SELECT
        ga_session_id,
        event_timestamp_pt AS landing_page_timestamp,
        page_title AS landing_page_title,
        page_location_page_type AS landing_page_type,
        page_location_path AS landing_page_path,
        page_location_url AS landing_page_url,
        page_location_query AS landing_page_query,
        page_location_host AS landing_page_host,
        traffic_source_medium,
        traffic_source_source,
        traffic_source_name
    FROM ga4_events
    WHERE is_landing_pageview
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
    FROM ga4_events
    WHERE is_exit_pageview
)

SELECT
    *
FROM page_counts
LEFT JOIN landing_pages
    USING (ga_session_id)
LEFT JOIN exit_pages
    USING (ga_session_id)
