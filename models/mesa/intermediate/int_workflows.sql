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

workflow_counts AS (
    SELECT
        workflow_id,
        COUNT_IF(workflow_steps.is_pro_app) > 0 AS has_pro_app,
        COUNT(DISTINCT workflow_steps.workflow_step_id) AS step_count,
        COUNT(DISTINCT deleted_workflow_steps.workflow_step_id) AS deleted_step_count,
        COALESCE(step_count, 0) + COALESCE(deleted_step_count, 0) AS step_count_with_deleted
    FROM workflows
    LEFT JOIN workflow_steps USING (workflow_id)
    LEFT JOIN deleted_workflow_steps USING (workflow_id)
    GROUP BY
        1
),

workflow_step_descriptions AS (
    SELECT
        workflow_id,
        LISTAGG(description, ' • ') WITHIN GROUP (ORDER BY step_type, step_weight, position_in_workflow) AS step_descriptions
    FROM workflow_steps
    GROUP BY 1
),

final AS (
    SELECT * EXCLUDE (deleted_app_chain, deleted_step_chain),
        COALESCE(app_chains.app_chain, deleted_app_chains.deleted_app_chain) AS app_chain_with_deleted,
        COALESCE(app_chains.step_chain, deleted_app_chains.deleted_step_chain) AS step_chain_with_deleted
    FROM workflows
    LEFT JOIN workflow_counts USING (workflow_id)
    LEFT JOIN app_chains USING (workflow_id)
    LEFT JOIN deleted_app_chains USING (workflow_id)
    LEFT JOIN workflow_step_descriptions USING (workflow_id)
)

SELECT *
FROM final
