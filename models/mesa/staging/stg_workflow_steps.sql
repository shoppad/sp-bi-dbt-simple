WITH workflow_steps AS (
    SELECT
        *
    FROM {{ source('mesa_mongo', 'mesa_workflow_steps') }}
    WHERE NOT(__hevo__marked_deleted) -- TODO: Should we filter out deleted workflow steps?

),

workflows AS (
    SELECT *
    FROM {{ ref('stg_workflows') }}
)

SELECT
    _id AS workflow_step_id,
    shop_id,
    shop_subdomain,
    "TYPE" AS integration_app,
    automation AS workflow_id,
    trigger_type,
    weight AS step_weight,
    ROW_NUMBER() OVER (PARTITION BY workflow_id, trigger_type ORDER BY weight) as position_in_workflow
FROM workflow_steps
LEFT JOIN workflows ON workflow_steps.automation = workflows.workflow_id
