WITH run_starts AS (
    SELECT
        {{ groomed_column_list(ref('stg_trigger_runs')) | join(',\n       ') }}
    FROM {{ ref('stg_trigger_runs') }}
),

step_runs AS (
    SELECT *
    FROM {{ ref('step_runs') }}
    ORDER BY
        workflow_run_id ASC,
        run_at_pt ASC
),
action_run_stats AS (
    {# TODO: Currently doesn't include all the Step Runs because they are missing the proper Parent. #}
    SELECT
        workflow_run_id,
        COUNT(*) AS executed_step_count,
        SUM(child_failure_count) AS child_failure_count, {# TODO: Does this only get set on the first step? #}
        LISTAGG(DISTINCT run_status) = 'success' AS is_successful,
        SPLIT_PART(LISTAGG(integration_app, ','), ',', -1) AS destination_app {# Hack to get the step's app. #}
    FROM step_runs
    GROUP BY
        workflow_run_id
),

final AS (
    SELECT
        *,
        CONCAT(source_app, '-', destination_app) AS source_destination_pair
    FROM run_starts
    LEFT JOIN action_run_stats USING (workflow_run_id)
)

SELECT * FROM final
