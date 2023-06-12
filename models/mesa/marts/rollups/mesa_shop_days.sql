WITH

shop_days AS (
    SELECT *
    FROM {{ ref('int_mesa_shop_days') }}

),

workflows AS (
    SELECT
        shop_subdomain,
        workflow_id,
        DATE_TRUNC(DAY, created_at_pt) AS dt,
        setup
    FROM {{ ref('workflows') }}
),

workflow_counts AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(COUNT(workflows.workflow_id), 0) AS workflows_created_count,
        COALESCE(COUNT_IF(workflows.setup IN ('incomplete', 'complete')), 0) AS workflows_wizard_started_count,
        COALESCE(COUNT_IF(workflows.setup = 'complete'), 0) AS workflows_wizard_complete_count,
        COALESCE(COUNT_IF(workflows.setup = 'incomplete'), 0) AS workflows_wizard_incomplete_count,
        COALESCE(COUNT_IF(workflows.setup IS NULL OR workflows.setup = 'false'), 0) AS workflows_created_without_wizard_count,
        workflows_created_count > 0 AS created_a_workflow,
        workflows_wizard_started_count > 0 AS started_wizard,
        workflows_wizard_complete_count > 0 AS completed_wizard,
        workflows_wizard_incomplete_count > 0 AS has_incomplete_wizard,
        workflows_created_without_wizard_count > 0 AS created_wowithout_wizard
    FROM shop_days
    LEFT JOIN workflows USING (shop_subdomain, dt)
    GROUP BY 1, 2
),

final AS (
    SELECT *
    FROM shop_days
    LEFT JOIN workflow_counts USING (shop_subdomain, dt)
)

SELECT * FROM final
