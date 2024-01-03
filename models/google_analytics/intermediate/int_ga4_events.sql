WITH
user_matching AS (SELECT * FROM {{ ref("stg_anonymous_to_known_user_matching") }}),

staged_ga4_events AS (
    SELECT *
    FROM {{ ref("stg_ga4_events") }}
),

final AS (

    SELECT
        user_matching.shop_subdomain,
        COALESCE(user_matching.shopify_id, staged_ga4_events.shopify_id) AS shopify_id,
        staged_ga4_events.*
        EXCLUDE (shopify_id, shop_subdomain)

    FROM staged_ga4_events
    LEFT JOIN user_matching
        ON
            (
                staged_ga4_events.user_pseudo_id = user_matching.user_pseudo_id
                OR
                staged_ga4_events.shopify_id::STRING = user_matching.shopify_id::STRING
                OR
                staged_ga4_events.shop_subdomain = user_matching.shop_subdomain
            )
)

SELECT *
FROM final
