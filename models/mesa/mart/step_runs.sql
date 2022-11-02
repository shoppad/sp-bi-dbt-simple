WITH
step_runs AS (
    SELECT *
    FROM {{ ref('stg_step_runs') }}
),

workflow_steps AS (
    SELECT *
    FROM {{ ref('stg_workflow_steps') }}
),

workflows AS (
    SELECT *
    FROM {{ ref('stg_workflows') }}
),

final AS (
    SELECT
        step_run_id,
        workflow_run_id,
        workflows.workflow_id,
        shop_id,
        shop_subdomain,
        run_at_utc,
        run_at_pt,
        DATE_TRUNC('day', run_at_pt)::date AS run_on_pt,
        trigger_type,
        status AS step_run_status,
        is_billable,
        is_free_workflow,
        unbillable_reason,
        child_failure_count,
        integration_app,
        ROW_NUMBER() OVER (PARTITION BY workflow_run_id ORDER BY run_at_pt) AS position_in_workflow_run
    FROM step_runs
    INNER JOIN workflows USING (workflow_id)
    LEFT JOIN workflow_steps USING (workflow_step_id)
)

SELECT *
FROM final
