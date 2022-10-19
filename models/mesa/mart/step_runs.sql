WITH
step_runs AS (
    SELECT *
    FROM {{ ref('stg_step_runs') }}
),

final AS (
    SELECT
        step_run_id,
        workflow_run_id,
        workflow_id,
        shop_id,
        shop_subdomain,
        run_at_utc,
        run_at_pt,
        trigger_type,
        status AS step_run_status,
        is_billable,
        is_free_workflow,
        unbillable_reason,
        child_failure_count,
        ROW_NUMBER() OVER (PARTITION BY workflow_run_id ORDER BY run_at_pt) AS workflow_sequence_index
    FROM step_runs
    INNER JOIN shops USING (shop_subdomain)
)

SELECT *
FROM final
