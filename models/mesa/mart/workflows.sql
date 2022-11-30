WITH workflows AS (

    SELECT *
    FROM {{ ref('stg_workflows') }}

),

workflow_steps AS (

    SELECT *
    FROM {{ ref('stg_workflow_steps') }}
    WHERE NOT(is_deleted)

),

workflow_runs AS (

    SELECT *
    FROM {{ ref('int_workflow_runs') }}

),

workflow_counts AS (
    SELECT
        workflow_id,
        COUNT(DISTINCT workflow_steps.*) AS step_count,
        MIN(
            IFF(workflow_runs.is_billable, workflow_runs.workflow_run_at_pt, NULL)
        ) AS first_run_at_pt,
        MIN(
            IFF((workflow_runs.is_billable AND workflow_runs.is_successful), workflow_runs.workflow_run_at_pt, NULL)
        ) AS first_successful_run_at_pt,
        COUNT(
            DISTINCT IFF(workflow_runs.is_billable, workflow_runs.workflow_run_id, NULL)
        ) AS run_attempt_count,
        COUNT(
            DISTINCT IFF((workflow_runs.is_billable AND workflow_runs.is_successful), workflow_runs.workflow_run_id, NULL)
            {# TODO: is is_successful appropriate here? Do failed filter run results result in something besides success? #}
        ) AS run_success_count
    FROM workflows
    LEFT JOIN workflow_steps USING (workflow_id)
    LEFT JOIN workflow_runs USING (workflow_id)
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
        test_attempt_count = 0 AS has_test_attempted_workflow,
        test_success_count = 0 AS has_test_succeeded_workflow
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
            COUNT_IF(event_id IN ('workflow_save', 'dashboard_workflow_edit') AND properties_workflow_id = workflow_id),
            0)
        AS save_count, {# TODO: Which event_ids do we want to count? #}
        save_count > 0 AS has_edited_or_saved_workflow
    FROM workflows
    LEFT JOIN {{ ref('int_mesa_flow_events') }} USING (shop_subdomain)
    GROUP BY 1

),

workflow_enables AS (

    SELECT
        workflow_id,
        COALESCE(COUNT_IF(event_id = 'workflow_enable' AND workflow_id = properties_workflow_id), 0) AS enable_count,
        enable_count > 0 AS has_enabled_workflow
    FROM workflows
    LEFT JOIN {{ ref('int_mesa_flow_events') }} USING (shop_subdomain)
    GROUP BY 1

),

final AS (

    SELECT *
    FROM workflows
    LEFT JOIN page_views USING (shop_subdomain, workflow_id)
    LEFT JOIN test_counts USING (workflow_id)
    LEFT JOIN workflow_saves USING (workflow_id)
    LEFT JOIN workflow_counts USING (workflow_id)
    LEFT JOIN workflow_enables USING (workflow_id)

)

SELECT * FROM final
