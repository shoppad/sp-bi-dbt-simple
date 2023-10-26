with
    shops as (select shop_subdomain, first_installed_at_pt from {{ ref("int_shops") }}),

    funnel_steps as (select * from {{ ref("mesa_funnel_steps") }}),

    workflow_achievements as (
        select
            shop_subdomain, achieved_at_pt, action as key, 'workflow_events' as source
        from {{ ref("int_workflow_event_achievements") }}

        union all

        select
            shop_subdomain, achieved_at_pt, action as key, 'mesa_flow_events' as source
        from {{ ref("int_mesa_flow_achievements") }}

        union all

        select
            shop_subdomain,
            first_installed_at_pt as achieved_at_pt,
            'installed_app' as key,
            'hardcoded_in_dbt' as source
        from shops
    ),

    funnel_achievements as (
        select shop_subdomain, funnel_steps.*, achieved_at_pt
        from funnel_steps
        left join workflow_achievements using (key, source)
        inner join shops using (shop_subdomain)
    )

select *
from funnel_achievements
