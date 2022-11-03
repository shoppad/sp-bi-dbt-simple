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
        step_runs.*,
        ROW_NUMBER() OVER (PARTITION BY workflow_run_id ORDER BY run_at_pt) AS position_in_workflow_run
    FROM step_runs
    INNER JOIN workflows USING (workflow_id)
    LEFT JOIN workflow_steps USING (workflow_step_id)
)

SELECT *
FROM final
