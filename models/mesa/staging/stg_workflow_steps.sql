WITH workflow_steps AS (
    SELECT *
    FROM {{ source('mongo_sync', 'workflow_steps') }}

),

workflows AS (
    SELECT *
    FROM {{ ref('stg_workflows') }}
)

SELECT
    _id AS workflow_step_id,
    shop_subdomain,
    "TYPE" AS integration_app,
    automation AS workflow_id,
    trigger_type AS step_type,
    weight AS step_weight,
    __hevo__marked_deleted AS is_deleted,
    ROW_NUMBER() OVER (PARTITION BY workflow_id, step_type ORDER BY weight) as position_in_workflow
FROM workflow_steps
LEFT JOIN workflows ON workflow_steps.automation = workflows.workflow_id
