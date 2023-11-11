with
    workflow_run_dates as (
        select
            shop_subdomain,
            min(workflow_run_on_pt) as source_first_dt,
            max(workflow_run_on_pt) as source_last_dt
        from {{ ref("int_workflow_runs") }}
        group by 1
    ),

    charge_dates as (
        select
            shop_subdomain,
            min(charged_on_pt) as source_first_dt,
            max(charged_on_pt) as source_last_dt
        from {{ ref("stg_mesa_charges") }}
        group by 1
    ),

    plan_dates as (
        select shop_subdomain, min(dt) as source_first_dt, max(dt) as source_last_dt
        from {{ ref("int_mesa_shop_plan_days") }}
        group by 1
    ),

    shop_dates as (
        select
            shop_subdomain,
            first_installed_at_pt as source_first_dt,
            case
                when is_shopify_zombie_plan
                then shopify_last_updated_at_pt
                when uninstalled_at_pt is null or status = 'active'
                then {{ pacific_timestamp("CURRENT_TIMESTAMP()") }}
                else uninstalled_at_pt
            end::date as source_last_dt
        from {{ ref("stg_shops") }}
    ),

    custom_app_revenue as (

        select shop_subdomain, first_dt as source_first_dt, last_dt as source_last_dt
        {# TODO: Add start/end dates to custom apps seed file. #}
        {# ?: Some custom apps can't connect to real stores. This probably means some Workflows aren't being attributed to a Store either. #}
        from {{ ref("custom_app_daily_revenues") }}

    ),

    combined_dates as (
        select
            shop_subdomain,
            min(source_first_dt) as source_first_dt,
            min_by(source, source_first_dt) as first_dt_source,
            max(source_last_dt) as source_last_dt,
            max_by(source, source_last_dt) as last_dt_source
        from
            (
                select *, 'charge_dates' as source
                from charge_dates
                union all
                select *, 'workflow_run_dates' as source
                from workflow_run_dates
                union all
                select *, 'shop_dates' as source
                from shop_dates
                union all
                select *, 'custom_app_revenue' as source
                from custom_app_revenue
                union all
                select *, 'plan_dates' as source
                from plan_dates
            )
        group by 1
    ),

    final as (
        select
            shop_subdomain,
            combined_dates.source_first_dt::date as first_dt,
            least(
                coalesce(
                    combined_dates.source_last_dt,
                    {{ pacific_timestamp("CURRENT_TIMESTAMP()") }}::date
                ),
                coalesce(
                    shop_dates.source_last_dt,
                    {{ pacific_timestamp("CURRENT_TIMESTAMP()") }}::date
                )
            ) as last_dt,
            {{
                datediff(
                    "first_dt",
                    "COALESCE(last_dt, "
                    ~ pacific_timestamp("CURRENT_TIMESTAMP()")
                    ~ ")::DATE",
                    "day",
                )
            }} + 1 as lifespan_length
        from combined_dates
        left join shop_dates using (shop_subdomain)  -- Added to override in case of uninstall.
    )

select *
from final
