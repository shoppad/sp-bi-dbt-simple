WITH workflow_steps AS (
    SELECT
        *
            RENAME
            _id AS workflow_step_id,
            automation AS workflow_id,
            trigger_type AS step_type,
            __hevo__marked_deleted AS is_deleted,
            weight AS step_weight,
            "TYPE" AS integration_app
    FROM {{ source('mongo_sync', 'workflow_steps') }}
),

workflows AS (
    SELECT *
    FROM {{ ref('stg_workflows') }}
),

final AS (
    SELECT
        workflow_step_id,
        workflow_id,
        shop_subdomain,
        step_type,
        is_deleted,
        integration_app,
        step_weight,
        {{ pacific_timestamp('_created_at') }} AS created_at_pt,
        NULLIF(metadata:description::STRING, '') AS description,
        IFF(is_deleted, 'DELETED - ' || workflow_steps.key, workflow_steps.key) AS step_key,
        workflow_steps.operation_id,
        CONCAT(integration_app::STRING || ' - ' || COALESCE(operation_id, integration_app::STRING)::STRING) AS step_name,
        REPLACE(REPLACE(IFF(is_deleted, 'DELETED - ' || trigger_name, trigger_name), 'In: ', ''), 'Out: ', '') AS step_custom_name,
        ROW_NUMBER() OVER (PARTITION BY workflow_id, step_type ORDER BY step_weight) as position_in_workflow,
        integration_app IN ('{{ var("pro_apps") | join("', '") }}') AS is_pro_app
    FROM workflow_steps
    LEFT JOIN workflows USING (workflow_id)
)

SELECT *
FROM final
