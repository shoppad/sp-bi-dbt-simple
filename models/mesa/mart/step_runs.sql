WITH
step_runs AS (
    SELECT *
    FROM {{ ref('int_step_runs') }}
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
    SELECT step_runs.*
    FROM step_runs
    INNER JOIN workflows USING (workflow_id)
    LEFT JOIN workflow_steps USING (workflow_step_id)
)

SELECT *
FROM final
