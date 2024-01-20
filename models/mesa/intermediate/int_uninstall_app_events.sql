WITH
shops AS (
    SELECT shop_subdomain
    FROM {{ ref('int_shops') }}
),

app_events AS (
    SELECT
        *
    FROM {{ ref('stg_shopify_partner_events')}}
),

uninstall_events AS (
    SELECT
        *
    FROM app_events
    WHERE event_type = 'RELATIONSHIP_UNINSTALLED'
)

SELECT *
FROM uninstall_events
INNER JOIN shops USING (shop_subdomain)
