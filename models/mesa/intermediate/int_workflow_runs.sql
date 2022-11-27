WITH run_starts AS (
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
        COUNT(*) AS executed_step_count,
        LISTAGG(DISTINCT run_status) = 'success' AS is_successful,
        SPLIT_PART(LISTAGG(integration_name, ',') WITHIN GROUP (ORDER BY position_in_workflow_run DESC), ',', 1) AS destination_app {# Hack to get the step's app. #}
    FROM step_runs
    GROUP BY
        workflow_run_id
),

final AS (
    SELECT
        *,
        source_app || '-' || destination_app AS source_destination_pair
    FROM run_starts
    LEFT JOIN action_run_stats USING (workflow_run_id)
)

SELECT * FROM final
