WITH workflow_runs AS (
    SELECT
        {{ groomed_column_list(ref('stg_workflow_runs')) | join(',\n       ') }}
    FROM {{ ref('stg_workflow_runs') }}
),

step_runs AS (
    SELECT *
    FROM {{ ref('int_step_runs') }}
),

action_run_stats AS (
    SELECT
        workflow_run_id,
        COALESCE(COUNT(*), 0) + 1 AS executed_step_count, -- Plus 1 for the workflow trigger itself
        SPLIT_PART(LISTAGG(integration_name, ',') WITHIN GROUP (ORDER BY position_in_workflow_run DESC), ',', 1) AS destination_app {# Hack to get the step's app. #}
    FROM workflow_runs
    LEFT JOIN step_runs USING (workflow_run_id)
    GROUP BY
        workflow_run_id
),

final AS (
    SELECT
        *,
        source_app || '-' || destination_app AS source_destination_pair,
        child_failure_count = 0 and run_status = 'success' AS is_successful
    FROM workflow_runs
    LEFT JOIN action_run_stats USING (workflow_run_id)
)

SELECT * FROM final