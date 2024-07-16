WITH workflows AS (

    SELECT *
    FROM {{ ref('int_workflows') }}

),

workflow_steps AS (

    SELECT *
    FROM {{ ref('stg_workflow_steps') }}
    WHERE NOT is_deleted
),

thirty_day_workflow_runs AS (
    SELECT *
    FROM {{ ref('int_workflow_runs') }}
    WHERE workflow_run_at_pt >= CURRENT_DATE - INTERVAL '30 days'
),

thirty_day_workflow_counts AS (
    SELECT
        workflow_id,
        SUM(executed_step_count) AS thirty_day_step_count,
        COUNT(
            DISTINCT IFF(thirty_day_workflow_runs.is_billable, thirty_day_workflow_runs.workflow_run_id, NULL)
        ) AS thirty_day_trigger_count,
        COUNT(
            DISTINCT IFF((thirty_day_workflow_runs.is_billable AND thirty_day_workflow_runs.is_successful), thirty_day_workflow_runs.workflow_run_id, NULL)
            {# ?: is is_successful appropriate here? Do failed filter runs result in something besides success? #}
        ) AS thirty_day_run_success_count,
        thirty_day_run_success_count / NULLIF(thirty_day_trigger_count, 0) AS thirty_day_run_success_percent,

        COUNT(
            DISTINCT IFF((thirty_day_workflow_runs.is_billable AND thirty_day_workflow_runs.did_move_data), thirty_day_workflow_runs.workflow_run_id, NULL)
        ) AS thirty_day_run_did_move_data_count,
        1.0 * thirty_day_run_did_move_data_count / NULLIF(thirty_day_trigger_count, 0) AS thirty_day_run_moved_data_percent,

        COUNT(
            DISTINCT IFF((thirty_day_workflow_runs.is_billable AND thirty_day_workflow_runs.was_filter_stopped), thirty_day_workflow_runs.workflow_run_id, NULL)
        ) AS thirty_day_run_was_filter_stopped_count,
        1.0 * thirty_day_run_was_filter_stopped_count / NULLIF(thirty_day_trigger_count, 0) AS thirty_day_run_was_filter_stopped_percent
    FROM workflows
    LEFT JOIN workflow_steps USING (workflow_id)
    LEFT JOIN thirty_day_workflow_runs USING (workflow_id)
    GROUP BY
        1
),


test_runs AS (

    SELECT *
    FROM {{ ref('int_test_runs') }}

),

test_counts AS (

    SELECT
        workflow_id,
        MIN(test_run_at_pt) AS first_test_at_pt,
        MIN(
            IFF(is_successful, test_run_at_pt, NULL)
        ) AS first_successful_test_at_pt,
        COALESCE(COUNT(DISTINCT test_runs.test_run_id), 0) AS test_attempt_count,
        COALESCE(COUNT_IF(test_runs.is_successful), 0) AS test_success_count,
        test_success_count / NULLIF(test_attempt_count, 0) AS test_success_percent,
        test_attempt_count > 0 AS has_test_attempted_workflow,
        test_success_count > 0 AS has_test_succeeded_workflow
    FROM workflows
    LEFT JOIN test_runs USING (workflow_id)
    GROUP BY 1

),

page_views AS (

    SELECT
        shop_subdomain,
        workflow_id,
        COALESCE(COUNT_IF(page_url_path LIKE 'automations/%' AND user_id = shop_subdomain), 0) AS page_view_count,
        page_view_count > 0 AS has_viewed_workflow
    FROM workflows
    LEFT JOIN {{ ref('segment_web_page_views__sessionized') }}
    GROUP BY 1, 2

),

workflow_saves AS (

    SELECT
        workflow_id,
        COALESCE(
            COUNT_IF(event_id IN ('workflow_save', 'dashboard_workflow_edit') AND workflow_id IN (properties_workflow_id, properties_id)),
            0)
            AS save_count,
        save_count > 0 AS has_edited_or_saved_workflow
    FROM workflows
    LEFT JOIN {{ ref('int_mesa_flow_events') }} USING (shop_subdomain)
    GROUP BY 1

),

workflow_enables AS (

    SELECT
        workflow_id,
        COALESCE(COUNT_IF(event_id = 'workflow_enable' AND workflow_id IN (properties_workflow_id, properties_id)), 0) AS enable_count,
        enable_count > 0 AS has_enabled_workflow
    FROM workflows
    LEFT JOIN {{ ref('int_mesa_flow_events') }} USING (shop_subdomain)
    GROUP BY 1
),

workflow_triggers AS (
    SELECT
        workflow_id,
        integration_app AS trigger_app,
        step_name AS trigger_step_name,
        operation_id AS trigger_operation_id
    FROM workflow_steps
    WHERE step_type = 'input'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY workflow_step_id) = 1
),

workflow_destinations AS (
    SELECT
        workflow_id,
        integration_app AS destination_app,
        step_name AS destination_step_name,
        operation_id AS destination_operation_id
    FROM workflow_steps
    WHERE step_type = 'output'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY workflow_step_id DESC) = 1
),

final AS (
    SELECT
        *,
        trigger_app || ' - ' || destination_app AS source_destination_pair,
        COALESCE(template_name IS NOT NULL AND template_name != '', FALSE) AS is_from_template,
        COALESCE(app_chain ILIKE ANY ('%googlesheets%', '%recharge%', '%infiniteoptions%', '%tracktor%', '%openai%', '%slack%'), FALSE) AS is_puc
    FROM workflows
    LEFT JOIN page_views USING (shop_subdomain, workflow_id)
    LEFT JOIN test_counts USING (workflow_id)
    LEFT JOIN workflow_saves USING (workflow_id)
    LEFT JOIN workflow_enables USING (workflow_id)
    LEFT JOIN thirty_day_workflow_counts USING (workflow_id)
    LEFT JOIN workflow_triggers USING (workflow_id)
    LEFT JOIN workflow_destinations USING (workflow_id)
)

SELECT *
FROM final
WHERE created_at_pt > '2024-05-01'
AND save_count > 0
