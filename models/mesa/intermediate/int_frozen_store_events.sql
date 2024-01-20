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

frozen_store_events AS (
    SELECT
        *
    FROM app_events
    WHERE event_type = 'SUBSCRIPTION_CHARGE_FROZEN'
)

SELECT *
FROM frozen_store_events
INNER JOIN shops USING (shop_subdomain)
