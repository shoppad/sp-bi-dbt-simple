WITH
ga4_events AS (
    SELECT *
    FROM {{ ref("stg_ga4_events") }}
),

shops AS (SELECT shop_subdomain, shopify_id FROM {{ ref("stg_shops") }}),

final AS (

    SELECT
        ga4_events.user_pseudo_id,
        shops.shopify_id::STRING AS shopify_id,
        shops.shop_subdomain
    FROM shops
    LEFT JOIN ga4_events
        ON (
            shops.shop_subdomain = ga4_events.shop_subdomain
            OR
            ga4_events.shopify_id::STRING = shops.shopify_id::STRING
        )
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY shops.shop_subdomain, user_pseudo_id
            ORDER BY true
        )
        = 1
)

SELECT *
FROM final
