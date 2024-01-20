WITH
shops AS (
    SELECT shop_subdomain
    FROM {{ ref('int_shops') }}
),

app_events AS (
    SELECT
        *
    FROM {{ ref('stg_shopify_partner_events') }}
),

deactivated_events AS (
    SELECT
        *
    FROM app_events
    WHERE event_type = 'RELATIONSHIP_DEACTIVATED'
),

max_reactivated_times AS (
    SELECT
        shop_subdomain,
        MAX(occurred_at) AS max_reactivated_at
    FROM app_events
    WHERE event_type = 'RELATIONSHIP_REACTIVATED'
    GROUP BY shop_subdomain
)

SELECT
    deactivated_events.shop_subdomain,
    deactivated_events.occurred_at
FROM deactivated_events
INNER JOIN shops
LEFT JOIN max_reactivated_times
    ON deactivated_events.shop_subdomain = shops.shop_subdomain
    AND deactivated_events.occurred_at > max_reactivated_at
