WITH
ga4_events AS (
    SELECT
        *
    FROM {{ ref("int_ga4_events") }}
),

shops AS (
    SELECT
        shop_subdomain,
        first_installed_at_pt AS shop_first_installed_at
    FROM {{ ref("int_shops") }}
),

final AS (
    SELECT
        ga4_events.*,
        COALESCE(event_timestamp_pt < shop_first_installed_at, TRUE) AS is_pre_install
    FROM ga4_events
    LEFT JOIN shops
        ON ga4_events.shop_subdomain = shops.shop_subdomain
)

SELECT *
FROM final
