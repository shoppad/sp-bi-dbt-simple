WITH workflows AS (

    SELECT *
    FROM {{ ref('stg_workflows') }}

),


workflow_steps AS (

    SELECT *
    FROM {{ ref('stg_workflow_steps') }}
    {# WHERE NOT is_deleted #}

),

first_workflow_first_steps AS (

    SELECT
        shop_subdomain,
        workflow_id AS first_workflow_id,
        integration_app AS first_workflow_trigger_app,
        step_key AS first_workflow_trigger_step_key,
        step_name AS first_workflow_trigger_step_name
    FROM workflows
    LEFT JOIN workflow_steps USING (workflow_id)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY workflow_steps.created_at_pt ASC) = 1

),

first_workflow_last_steps AS (

    SELECT
        workflow_id AS first_workflow_id,
        integration_app AS first_workflow_last_step_app,
        step_key AS first_workflow_last_step_key,
        step_name AS first_workflow_last_step_name
    FROM workflow_steps
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY created_at_pt DESC) = 1

),


first_workflow_keys AS (

    SELECT
        * EXCLUDE (first_workflow_id)
    FROM first_workflow_first_steps
    LEFT JOIN first_workflow_last_steps USING (first_workflow_id)

)

SELECT *
FROM first_workflow_keys
