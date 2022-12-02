WITH
shops AS (
    SELECT * FROM {{ ref('int_shops') }}
),

mesa_workflow_events AS (
    SELECT * FROM {{ ref('stg_workflow_events') }}
),

step_achievements AS (
    SELECT
        shop_subdomain,
        action,
        {{ pacific_timestamp('MIN(timestamp)') }} AS achieved_at_pt
    FROM shops
    INNER JOIN mesa_workflow_events USING (shop_subdomain)
    GROUP BY 1, 2
)

SELECT * FROM step_achievements
