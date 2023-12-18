WITH
shop_anonymous_keys AS (
    SELECT * FROM {{ ref("stg_anonymous_to_known_user_matching") }}
),

ga4_events AS (
    SELECT * FROM {{ ref("stg_ga4_events") }}
),

final AS (
    SELECT
        ga4_events.* EXCLUDE (shop_subdomain, shopify_id),
        shop_anonymous_keys.shop_subdomain,
        shop_anonymous_keys.shopify_id,

                -- URL parts
        PARSE_URL(page_location) AS parsed_url,
        parsed_url:host || '/' || parsed_url:path AS page_url,
        parsed_url:host::STRING AS page_host,
        '/' || parsed_url:path::STRING AS page_path,
        '?' || parsed_url:query::STRING AS page_query
    FROM ga4_events
    LEFT JOIN shop_anonymous_keys USING (user_pseudo_id)
)

SELECT * EXCLUDE (parsed_url)
FROM final
