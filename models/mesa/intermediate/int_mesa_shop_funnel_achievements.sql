WITH
shops AS (
    SELECT * FROM {{ ref('int_shops') }}
),

funnel_steps AS (
    SELECT *
    FROM {{ ref('mesa_funnel_steps') }}
),

workflow_achievements AS (
    SELECT
        shop_subdomain,
        achieved_at_pt,
        action AS key
    FROM {{ ref('int_workflow_event_achievements') }}
),

funnel_achievements AS (
    SELECT
        shop_subdomain,
        funnel_steps.*,
        achieved_at_pt
    FROM funnel_steps
    LEFT JOIN workflow_achievements USING (key)
    INNER JOIN shops USING (shop_subdomain)
)

SELECT * FROM funnel_achievements
