WITH workflows AS (

    SELECT *
    FROM {{ ref('stg_workflows') }}

),
workflow_steps AS (

    SELECT *
    FROM {{ ref('stg_workflow_steps') }}
    WHERE NOT is_deleted

),

workflow_runs AS (

    SELECT *
    FROM {{ ref('int_workflow_runs') }}
    WHERE NOT is_time_travel

),

app_chains AS (

    SELECT
        workflow_id,
        LISTAGG(integration_app, ' • ') WITHIN GROUP (ORDER BY step_type ASC, position_in_workflow ASC) AS app_chain,
        LISTAGG(CONCAT(integration_app, ' → ' , step_name), ' • ') WITHIN GROUP (ORDER BY step_type ASC, position_in_workflow ASC) AS step_chain
    FROM workflow_steps
    GROUP BY 1

),

workflow_counts AS (
    SELECT
        workflow_id,
        COUNT_IF(workflow_steps.is_pro_app) > 0 AS has_pro_app,
        COUNT(DISTINCT workflow_steps.workflow_step_id) AS step_count,
        MIN(
            IFF(workflow_runs.is_billable, workflow_runs.workflow_run_at_pt, NULL)
        ) AS first_run_at_pt,
        MIN(
            IFF((workflow_runs.is_billable AND workflow_runs.is_successful), workflow_runs.workflow_run_at_pt, NULL)
        ) AS first_successful_run_at_pt,
        COUNT(
            DISTINCT IFF(workflow_runs.is_billable, workflow_runs.workflow_run_id, NULL)
        ) AS trigger_count,

        COUNT(
            DISTINCT IFF((workflow_runs.is_billable AND workflow_runs.is_successful), workflow_runs.workflow_run_id, NULL)
            {# ?: is is_successful appropriate here? Do failed filter runs result in something besides success? #}
        ) AS run_success_count,
        1.0 * run_success_count / NULLIF(trigger_count, 0) AS run_success_percent,

        COUNT(
            DISTINCT IFF((workflow_runs.is_billable AND workflow_runs.did_move_data), workflow_runs.workflow_run_id, NULL)
        ) AS run_did_move_data_count,
        1.0 * run_did_move_data_count / NULLIF(trigger_count, 0) AS run_moved_data_percent,

        COUNT(
            DISTINCT IFF((workflow_runs.is_billable AND workflow_runs.was_filter_stopped), workflow_runs.workflow_run_id, NULL)
        ) AS run_was_filter_stopped_count,
        1.0 * run_was_filter_stopped_count / NULLIF(trigger_count, 0) AS run_was_filter_stopped_percent
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
    LEFT JOIN app_chains USING (workflow_id)
)

SELECT *
FROM final
