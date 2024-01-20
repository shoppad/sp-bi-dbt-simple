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

cancelled_charge_events AS (
    SELECT
        *
    FROM app_events
    WHERE event_type = 'SUBSCRIPTION_CHARGE_CANCELED'
),

cancelleds_from_uninstalls AS (
    SELECT
        shop_subdomain,
        cancelled_charge_events.occurred_at
            BETWEEN
                int_uninstall_app_events.occurred_at - INTERVAL '1hour'
                AND
                int_uninstall_app_events.occurred_at + INTERVAL '1hour' AS is_cancelled_from_uninstall
    FROM cancelled_charge_events
    LEFT JOIN {{ ref('int_uninstall_app_events') }} USING (shop_subdomain)
)

SELECT *
FROM cancelled_charge_events
LEFT JOIN cancelleds_from_uninstalls USING (shop_subdomain)
INNER JOIN shops USING (shop_subdomain)
