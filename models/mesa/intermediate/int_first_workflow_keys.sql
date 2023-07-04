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
        step_key AS first_workflow_trigger_key,
        step_name AS first_workflow_trigger_name,
        workflow_step_id AS first_workflow_trigger_step_id,
        is_deleted AS first_workflow_title,
        IFF(is_deleted, 'DELETED - ' || title, title) AS first_workflow_sort_title
    FROM workflows
    LEFT JOIN workflow_steps USING (workflow_id)
        WHERE step_type = 'input'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY workflow_steps.created_at_pt ASC) = 1

),

first_workflow_last_steps AS (

    SELECT
        workflow_id AS first_workflow_id,
        integration_app AS first_workflow_destination_app,
        step_key AS first_workflow_destination_key,
        step_name AS first_workflow_destination_name
    FROM workflow_steps
    WHERE
        step_type = 'output'
        AND workflow_step_id NOT IN (
            SELECT first_workflow_trigger_step_id
            FROM first_workflow_first_steps
        )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY position_in_workflow DESC) = 1

),

final AS (

    SELECT
        * EXCLUDE (first_workflow_id, first_workflow_trigger_step_id),
        first_workflow_trigger_app || ' - ' || first_workflow_destination_app AS trigger_destination_app_pair,
        first_workflow_trigger_key || ' - ' || first_workflow_destination_key AS trigger_destination_key_pair,
        first_workflow_trigger_name || ' - ' || first_workflow_destination_name AS trigger_destination_name_pair

    FROM first_workflow_first_steps
    LEFT JOIN first_workflow_last_steps USING (first_workflow_id)

)

SELECT *
FROM final
