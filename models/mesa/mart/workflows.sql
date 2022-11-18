{{ config(materialized = 'table') }}

WITH workflows AS (
    SELECT *
    FROM {{ ref('stg_workflows') }}
),

workflow_steps AS (
    SELECT *
    FROM {{ ref('stg_workflow_steps') }}
),

workflow_runs AS (
    SELECT *
    FROM {{ ref('workflow_runs') }}
),

workflow_counts AS (
    SELECT
        workflow_id,
        COUNT(*) AS step_count,
        MIN(
            IFF(workflow_runs.is_billable, workflow_runs.workflow_run_at_pt, NULL)
        ) AS first_run_at_pt,
        MIN(
            IFF((workflow_runs.is_billable AND workflow_runs.is_successful), workflow_runs.workflow_run_at_pt, NULL)
        ) AS first_successful_run_at_pt,
        COUNT(
            IFF(workflow_runs.is_billable, workflow_runs.workflow_run_id, NULL)
        ) AS run_start_count,
        COUNT(
            IFF((workflow_runs.is_billable AND workflow_runs.is_successful), workflow_runs.workflow_run_id, NULL)
            {# TODO: is is_successful appropriate here? Do failed filter run results result in something besides success? #}
        ) AS run_success_count
    FROM workflows
    LEFT JOIN workflow_steps USING (workflow_id)
    LEFT JOIN workflow_runs USING (workflow_id)
    GROUP BY
        1
),

final AS (
    SELECT *
    FROM workflows
    LEFT JOIN workflow_counts USING (workflow_id)
)

SELECT * FROM final
