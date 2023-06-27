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
        integration_app AS source_step_app,
        step_key AS source_step_key,
        step_name AS source_step_name,
        workflow_step_id AS source_step_id,
        title AS first_workflow_title
    FROM workflows
    LEFT JOIN workflow_steps USING (workflow_id)
        WHERE step_type = 'input'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY workflow_steps.created_at_pt ASC) = 1

),

first_workflow_last_steps AS (

    SELECT
        workflow_id AS first_workflow_id,
        integration_app AS destination_step_app,
        step_key AS destination_step_key,
        step_name AS destination_step_name
    FROM workflow_steps
    WHERE
        step_type = 'output'
        AND workflow_step_id NOT IN (
            SELECT source_step_id
            FROM first_workflow_first_steps
        )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY position_in_workflow DESC) = 1

),

final AS (

    SELECT
        * EXCLUDE (first_workflow_id, source_step_id),
        source_step_app || ' - ' || destination_step_app AS source_destination_app_pair,
        source_step_key || ' - ' || destination_step_key AS source_destination_key_pair,
        source_step_name || ' - ' || destination_step_name AS source_destination_name_pair

    FROM first_workflow_first_steps
    LEFT JOIN first_workflow_last_steps USING (first_workflow_id)

)

SELECT *
FROM final
