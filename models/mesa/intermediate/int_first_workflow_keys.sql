with
    workflows as (select * from {{ ref("int_workflows") }}),

    workflow_steps as (

        select * from {{ ref("stg_workflow_steps") }}
    {# WHERE NOT is_deleted #}
    ),

    first_workflows as (
        select *
        from workflows
        where step_count > 1
        qualify
            row_number() over (partition by shop_subdomain order by created_at_pt asc)
            = 1
    ),

    first_workflow_first_steps as (

        select
            shop_subdomain,
            workflow_id as first_workflow_id,
            integration_app as first_workflow_trigger_app,
            step_key as first_workflow_trigger_key,
            operation_id as first_workflow_trigger_operation_id,
            step_name as first_workflow_trigger_name,
            workflow_step_id as first_workflow_trigger_step_id,
            title as first_workflow_title,
            iff(is_deleted, 'DELETED - ' || title, title) as first_workflow_sort_title,
            app_chain as first_workflow_app_chain,
            step_chain as first_workflow_step_chain
        from first_workflows
        left join workflow_steps using (workflow_id)
        where workflow_steps.step_type = 'input'
        qualify
            row_number() over (
                partition by workflow_id order by position_in_workflow desc
            )
            = 1
    ),

    first_workflow_last_steps as (

        select
            workflow_id as first_workflow_id,
            integration_app as first_workflow_destination_app,
            step_key as first_workflow_destination_key,
            operation_id as first_workflow_destination_operation_id,
            step_name as first_workflow_destination_name
        from workflow_steps
        where
            step_type = 'output'
            and workflow_step_id not in (
                select first_workflow_trigger_step_id from first_workflow_first_steps
            )
        qualify
            row_number() over (
                partition by workflow_id order by position_in_workflow desc
            )
            = 1
    ),

    final as (

        select
            * exclude first_workflow_trigger_step_id,
            first_workflow_trigger_app
            || ' - '
            || first_workflow_destination_app
            as first_workflow_trigger_destination_app_pair,
            first_workflow_trigger_key
            || ' - '
            || first_workflow_destination_key
            as first_workflow_trigger_destination_key_pair,
            first_workflow_trigger_name
            || ' - '
            || first_workflow_destination_name
            as first_workflow_trigger_destination_name_pair

        from first_workflow_first_steps
        left join first_workflow_last_steps using (first_workflow_id)

    )

select *
from final
