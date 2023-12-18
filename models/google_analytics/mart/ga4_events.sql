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
        SPLIT_PART(page_location, '//', 2) AS page_url,
        SPLIT_PART(page_url, '/', 1) AS page_host,
        SPLIT_PART(page_url, '?', 1) AS page_path


    FROM ga4_events
    LEFT JOIN shop_anonymous_keys USING (user_pseudo_id)
)

SELECT *
FROM final
