WITH workflows AS (

    SELECT *
    FROM {{ ref('stg_workflows') }}

),
workflow_steps AS (

    SELECT *
    FROM {{ ref('stg_workflow_steps') }}
    WHERE NOT is_deleted

),

app_chains AS (

    SELECT
        workflow_id,
        LISTAGG(integration_app, ' • ') WITHIN GROUP (ORDER BY step_type, position_in_workflow ASC) AS app_chain,
        LISTAGG(CONCAT(integration_app, ' →  ' , step_name), ' • ') WITHIN GROUP (ORDER BY step_type, position_in_workflow ASC) AS step_chain
    FROM workflow_steps
    GROUP BY 1

),

final AS (
    SELECT *
    FROM workflows
    LEFT JOIN app_chains USING (workflow_id)
)

SELECT *
FROM final AS
