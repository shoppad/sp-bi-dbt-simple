WITH source AS (
      SELECT * FROM {{ source('mongo_sync', 'temp_shopify_partner_events') }}
),

split_uninstalls AS (
    SELECT
        shop AS shop_subdomain,
        type AS event_type,
        occurredat AS occurred_at,
        {# {{ pacific_timestamp('occurredat') }} AS occurred_at, #}
        value::STRING AS reason,
        description
    FROM source,
    LATERAL FLATTEN(INPUT => SPLIT(reason, ','))
    WHERE type = 'RELATIONSHIP_UNINSTALLED'
),

other_events AS (
    SELECT
        shop AS shop_subdomain,
        type AS event_type,
        occurredat AS occurred_at,
        reason,
        description
    FROM source
    WHERE type != 'RELATIONSHIP_UNINSTALLED'
),

combined AS (

    SELECT * FROM split_uninstalls
    UNION ALL
    SELECT * FROM other_events
)

SELECT *
FROM combined
ORDER BY occurred_at ASC
