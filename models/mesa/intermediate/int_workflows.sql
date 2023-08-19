WITH workflows AS (

    SELECT *
    FROM {{ ref('stg_workflows') }}

),
workflow_steps AS (

    SELECT *
    FROM {{ ref('stg_workflow_steps') }}
    WHERE NOT is_deleted

),

deleted_workflow_steps AS (

        SELECT *
        FROM {{ ref('stg_workflow_steps') }}
        WHERE is_deleted
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

deleted_app_chains AS (
    SELECT
        workflow_id,
        LISTAGG(integration_app, ' • ') WITHIN GROUP (ORDER BY step_type ASC, position_in_workflow ASC) AS deleted_app_chain,
        LISTAGG(CONCAT(integration_app, ' → ' , step_name), ' • ') WITHIN GROUP (ORDER BY step_type ASC, position_in_workflow ASC) AS deleted_step_chain
    FROM deleted_workflow_steps
    GROUP BY 1
),

workflow_counts AS (
    SELECT
        workflow_id,
        COUNT_IF(workflow_steps.is_pro_app) > 0 AS has_pro_app,
        COUNT(DISTINCT workflow_steps.workflow_step_id) AS step_count,
        COUNT(DISTINCT deleted_workflow_steps.workflow_step_id) AS deleted_step_count,
        COALESCE(step_count, 0) + COALESCE(deleted_step_count, 0) AS step_count_with_deleted,
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
    LEFT JOIN deleted_workflow_steps USING (workflow_id)
    GROUP BY
        1
),

final AS (
    SELECT * EXCLUDE (deleted_app_chain, deleted_step_chain),
        COALESCE(app_chains.app_chain, deleted_app_chains.deleted_app_chain) AS app_chain_with_deleted,
        COALESCE(app_chains.step_chain, deleted_app_chains.deleted_step_chain) AS step_chain_with_deleted
    FROM workflows
    LEFT JOIN workflow_counts USING (workflow_id)
    LEFT JOIN app_chains USING (workflow_id)
    LEFT JOIN deleted_app_chains USING (workflow_id)
)

SELECT *
FROM final
