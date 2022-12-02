WITH
shops AS (
    SELECT * FROM {{ ref('int_shops') }}
),

mesa_flow_events AS (
    SELECT * FROM {{ ref('int_mesa_flow_events') }}
),

step_achievements AS (
    SELECT
        shop_subdomain,
        event_id AS action,
        {{ pacific_timestamp('MIN(timestamp)') }} AS achieved_at_pt
    FROM shops
    INNER JOIN mesa_flow_events USING (shop_subdomain)
    GROUP BY 1, 2
)

SELECT * FROM step_achievements
