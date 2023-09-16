WITH
workflow_runs AS (
    SELECT
        {{ groomed_column_list(ref('stg_workflow_runs'), except=['is_test_run']) | join(',\n       ') }}
    FROM {{ ref('stg_workflow_runs') }}
    WHERE NOT(is_test_run)
),

step_runs AS (
    SELECT *
    FROM {{ ref('int_step_runs') }}
),

action_run_stats AS (
    SELECT
        workflow_run_id,
        COALESCE(COUNT(*), 0) + 1 AS executed_step_count, -- Plus 1 for the workflow trigger itself
        MAX_BY(integration_name, position_in_workflow_run) AS destination_app
    FROM workflow_runs
    LEFT JOIN step_runs USING (workflow_run_id)
    GROUP BY
        workflow_run_id
),

final AS (
    SELECT
        *,
        source_app || ' - ' || destination_app AS source_destination_pair,
        COALESCE(child_failure_count = 0, TRUE) AND run_status = 'success' AS is_successful,
        COALESCE(run_status = 'fail', FALSE) AS is_failure,
        COALESCE(child_stop_count > 0, FALSE) AS is_stop,
        destination_app ILIKE '%delay%' AS did_end_with_delay,
        child_complete_count > 0 AND NOT did_end_with_delay AS did_move_data,
        COALESCE(child_complete_count = 0 AND child_stop_count > 0, FALSE) AS was_filter_stopped
    FROM workflow_runs
    LEFT JOIN action_run_stats USING (workflow_run_id)
)

SELECT * FROM final
