WITH

shop_days AS (

    SELECT *
    FROM {{ ref('int_mesa_shop_days') }}

),

workflow_creations AS (
    SELECT
        shop_subdomain,
        date_trunc('day', created_at_pt) AS dt,
        COUNT(workflows.*) AS workflows_created_count,
        COUNT_IF(workflows.setup IN ('incomplete', 'complete')) AS workflows_wizard_started_count,
        COUNT_IF(workflows.setup = 'complete') AS workflows_wizard_complete_count,
        COUNT_IF(workflows.setup = 'incomplete') AS workflows_wizard_incomplete_count
    FROM {{ ref('workflows') }}
    GROUP BY 1, 2
),

final AS (

    SELECT *
    FROM shop_days
    LEFT JOIN workflow_creations USING (shop_subdomain, dt)
)

SELECT * FROM final
