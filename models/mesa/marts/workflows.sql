WITH

{# ========== Base Data ========== #}

stg_workflows AS (
    SELECT * FROM {{ ref('stg_workflows') }}
),

workflow_steps AS (
    SELECT * FROM {{ ref('stg_workflow_steps') }}
    WHERE NOT is_deleted
),

deleted_workflow_steps AS (
    SELECT * FROM {{ ref('stg_workflow_steps') }}
    WHERE is_deleted
),

{# ========== App Chains ========== #}

app_chains AS (
    SELECT
        workflow_id,
        LISTAGG(integration_app, ' • ') WITHIN GROUP (ORDER BY step_type ASC, position_in_workflow ASC) AS app_chain,
        LISTAGG(step_name, ' • ') WITHIN GROUP (ORDER BY step_type ASC, position_in_workflow ASC) AS step_chain
    FROM workflow_steps
    GROUP BY 1
),

deleted_app_chains AS (
    SELECT
        workflow_id,
        LISTAGG(integration_app, ' • ') WITHIN GROUP (ORDER BY step_type ASC, position_in_workflow ASC) AS deleted_app_chain,
        LISTAGG(CONCAT(integration_app, ' → ', step_name), ' • ') WITHIN GROUP (ORDER BY step_type ASC, position_in_workflow ASC) AS deleted_step_chain
    FROM deleted_workflow_steps
    GROUP BY 1
),

{# ========== Workflow Counts ========== #}

workflow_counts AS (
    SELECT
        workflow_id,
        COUNT_IF(workflow_steps.is_pro_app) > 0 AS has_pro_app,
        COUNT(DISTINCT workflow_steps.workflow_step_id) AS step_count,
        COUNT(DISTINCT deleted_workflow_steps.workflow_step_id) AS deleted_step_count,
        COALESCE(step_count, 0) + COALESCE(deleted_step_count, 0) AS step_count_with_deleted
    FROM stg_workflows
    LEFT JOIN workflow_steps USING (workflow_id)
    LEFT JOIN deleted_workflow_steps USING (workflow_id)
    GROUP BY 1
),

{# ========== Step Descriptions ========== #}

workflow_step_descriptions AS (
    SELECT
        workflow_id,
        LISTAGG(description, ' • ') WITHIN GROUP (ORDER BY step_type, step_weight, position_in_workflow) AS step_descriptions
    FROM workflow_steps
    GROUP BY 1
),

{# ========== Triggers & Destinations ========== #}

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

{# ========== Final ========== #}

final AS (
    SELECT
        stg_workflows.*,
        workflow_counts.has_pro_app,
        workflow_counts.step_count,
        workflow_counts.deleted_step_count,
        workflow_counts.step_count_with_deleted,
        app_chains.app_chain,
        app_chains.step_chain,
        COALESCE(app_chains.app_chain, deleted_app_chains.deleted_app_chain) AS app_chain_with_deleted,
        COALESCE(app_chains.step_chain, deleted_app_chains.deleted_step_chain) AS step_chain_with_deleted,
        workflow_step_descriptions.step_descriptions,
        trigger_app,
        trigger_step_name,
        trigger_operation_id,
        destination_app,
        destination_step_name,
        destination_operation_id,
        trigger_app || ' - ' || destination_app AS source_destination_pair,
        COALESCE(template_name IS NOT NULL AND template_name != '', FALSE) AS is_from_template,
        COALESCE(app_chain ILIKE ANY ('%googlesheets%', '%recharge%', '%infiniteoptions%', '%tracktor%', '%openai%', '%slack%'), FALSE) AS is_puc
    FROM stg_workflows
    LEFT JOIN workflow_counts USING (workflow_id)
    LEFT JOIN app_chains USING (workflow_id)
    LEFT JOIN deleted_app_chains USING (workflow_id)
    LEFT JOIN workflow_step_descriptions USING (workflow_id)
    LEFT JOIN workflow_triggers USING (workflow_id)
    LEFT JOIN workflow_destinations USING (workflow_id)
)

SELECT *
FROM final
