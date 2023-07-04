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
    {{ pacific_timestamp('_created_at') }} AS created_at_pt,
    "TYPE" AS integration_app,
    __hevo__marked_deleted AS is_deleted,
    IFF(is_deleted, 'DELETED - ' || workflow_steps.key, workflow_steps.key) AS step_key,
    IFF(is_deleted, 'DELETED - ' || trigger_name, trigger_name) AS step_name,
    automation AS workflow_id,
    trigger_type AS step_type,
    weight AS step_weight,
    ROW_NUMBER() OVER (PARTITION BY workflow_id, step_type ORDER BY weight) as position_in_workflow,
    integration_app IN ('{{ var("pro_apps") | join("', '") }}') AS is_pro_app

FROM workflow_steps
LEFT JOIN workflows ON workflow_steps.automation = workflows.workflow_id
