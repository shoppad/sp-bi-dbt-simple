WITH

ga4_events AS (
    SELECT
        *
        EXCLUDE (
            event_timestamp,
            __hevo__ingested_at,
            __hevo__loaded_at
        )
        RENAME (
            name AS traffic_source_name,
            medium AS traffic_source_medium,
            source AS traffic_source_source,
            category AS device_category,
            __hevo_id AS event_id,
            user_id AS shop_subdomain,
            shop_id AS shopify_id,
            surface_detail AS app_store_surface_detail,
            surface_type AS app_store_surface_type
        ),
        {{ pacific_timestamp('TO_TIMESTAMP(event_timestamp)') }} AS event_timestamp_pt
    FROM {{ source('mesa_ga4', 'events') }}
    WHERE ga_session_id IS NOT NULL
)

SELECT *
FROM ga4_events
