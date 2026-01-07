WITH workflows AS (

    SELECT *
    FROM {{ ref('int_workflows') }}

),

workflow_steps AS (

    SELECT *
    FROM {{ ref('stg_workflow_steps') }}
    WHERE NOT is_deleted
),

page_views AS (

    SELECT
        shop_subdomain,
        workflow_id,
        0 AS page_view_count, -- Segment data removed
        FALSE AS has_viewed_workflow
    FROM workflows

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
        workflows.*,
        page_view_count,
        has_viewed_workflow,
        save_count,
        has_edited_or_saved_workflow,
        enable_count,
        has_enabled_workflow,
        trigger_app,
        trigger_step_name,
        trigger_operation_id,
        destination_app,
        destination_step_name,
        destination_operation_id,
        trigger_app || ' - ' || destination_app AS source_destination_pair,
        COALESCE(template_name IS NOT NULL AND template_name != '', FALSE) AS is_from_template,
        COALESCE(app_chain ILIKE ANY ('%googlesheets%', '%recharge%', '%infiniteoptions%', '%tracktor%', '%openai%', '%slack%'), FALSE) AS is_puc
    FROM workflows
    LEFT JOIN page_views USING (shop_subdomain, workflow_id)
    LEFT JOIN workflow_saves USING (workflow_id)
    LEFT JOIN workflow_enables USING (workflow_id)
    LEFT JOIN workflow_triggers USING (workflow_id)
    LEFT JOIN workflow_destinations USING (workflow_id)
)

SELECT *
FROM final
