with

    calendar_dates as (select date_day as dt from {{ ref("calendar_dates") }}),

    mesa_plan_changes as (
        select
            shop_subdomain,
            interval as mesa_plan_interval,
            changed_on_pt as dt,
            price as mesa_plan_interval_price,
            plan as mesa_plan,
            planid as mesa_plan_identifier,
            row_number() over (
                partition by shop_subdomain order by changed_at_pt asc
            ) as change_order
        from {{ ref("stg_mesa_plan_changes") }}
    ),

    mesa_plan_calendar_dates as (
        select
            mesa_plan_changes.shop_subdomain,
            calendar_dates.dt,
            mesa_plan_changes.mesa_plan,
            mesa_plan_changes.mesa_plan_identifier,
            mesa_plan_changes.mesa_plan_interval_price,
            mesa_plan_changes.mesa_plan_interval,
            case
                when mesa_plan_changes.mesa_plan_interval = 'annual'
                then mesa_plan_changes.mesa_plan_interval_price / 365
                when mesa_plan_changes.mesa_plan_interval = 'monthly'
                then
                    mesa_plan_changes.mesa_plan_interval_price
                    / day(last_day(calendar_dates.dt))
            end as daily_plan_revenue
        from mesa_plan_changes
        left join
            mesa_plan_changes as next_plan_changes
            on mesa_plan_changes.shop_subdomain = next_plan_changes.shop_subdomain
            and mesa_plan_changes.change_order + 1 = next_plan_changes.change_order
        inner join
            calendar_dates
            on calendar_dates.dt between mesa_plan_changes.dt and coalesce(
                next_plan_changes.dt - interval '1day', current_date
            )
    ),

    shopify_plan_changes as (
        select
            shop_subdomain,
            changed_on_pt as dt,
            plan as shopify_plan,
            oldplan as old_shopify_plan,
            coalesce(
                plan
                in ({{ "'" ~ var("zombie_store_shopify_plans") | join("', '") ~ "'" }}),
                false
            ) as is_zombie,
            row_number() over (
                partition by shop_subdomain order by changed_at_pt asc
            ) as change_order
        from {{ ref("stg_shopify_plan_changes") }}
    ),

    initial_shopify_plan_simulations as (
        select
            stg_shops.shop_subdomain,
            first_installed_on_pt as dt,
            coalesce(
                shopify_plan_changes.old_shopify_plan,
                analytics:initial:shopify_plan_name
            ) as shopify_plan,
            null as old_shopify_plan,
            coalesce(
                old_shopify_plan
                in ({{ "'" ~ zombie_store_shopify_plans | join("', '") ~ "'" }}),
                false
            ) as is_zombie,
            0 as change_order
        from {{ ref("stg_shops") }}
        left join
            shopify_plan_changes
            on stg_shops.shop_subdomain = shopify_plan_changes.shop_subdomain
            and change_order = 1
    ),

    shopify_plan_calendar_dates as (
        select
            combined_shopify_plan_changes.* exclude (
                dt, old_shopify_plan, change_order
            ),
            calendar_dates.dt
        from
            (
                select *
                from shopify_plan_changes

                union all

                select *
                from initial_shopify_plan_simulations
            ) as combined_shopify_plan_changes
        left join
            shopify_plan_changes as next_shopify_plan_changes
            on combined_shopify_plan_changes.shop_subdomain
            = next_shopify_plan_changes.shop_subdomain
            and combined_shopify_plan_changes.change_order + 1
            = next_shopify_plan_changes.change_order
        inner join
            calendar_dates
            on calendar_dates.dt between combined_shopify_plan_changes.dt and coalesce(
                next_shopify_plan_changes.dt - interval '1day', current_date
            )
        order by combined_shopify_plan_changes.change_order asc
    ),

    custom_app_daily_revenue as (
        select
            shop_subdomain,
            daily_plan_revenue as custom_daily_plan_revenue,
            first_dt,
            last_dt
        from {{ ref("custom_app_daily_revenues") }}
    ),

    custom_app_daily_revenue_dates as (
        select
            shop_subdomain,
            dt,
            custom_daily_plan_revenue,
            'custom-app' as mesa_plan,
            'custom-app' as mesa_plan_identifier,
            custom_daily_plan_revenue as mesa_plan_interval_price,
            'day' as mesa_plan_interval
        from custom_app_daily_revenue
        inner join
            calendar_dates
            on calendar_dates.dt between custom_app_daily_revenue.first_dt and coalesce(
                custom_app_daily_revenue.last_dt,
                {{ pacific_timestamp("current_timestamp()") }}::date
            )
    ),

    final as (
        select
            dt,
            shop_subdomain,
            coalesce(iff(is_zombie, 0, daily_plan_revenue), 0)
            + coalesce(custom_daily_plan_revenue, 0) as daily_plan_revenue,
            coalesce(
                mesa_plan_calendar_dates.mesa_plan,
                custom_app_daily_revenue_dates.mesa_plan
            ) as mesa_plan,
            coalesce(
                mesa_plan_calendar_dates.mesa_plan_identifier,
                custom_app_daily_revenue_dates.mesa_plan_identifier
            ) as mesa_plan_identifier,
            shopify_plan,
            is_zombie,
            coalesce(is_zombie, false) as is_shopify_zombie_plan
        from mesa_plan_calendar_dates
        full outer join custom_app_daily_revenue_dates using (shop_subdomain, dt)
        left join shopify_plan_calendar_dates using (shop_subdomain, dt)
    )

select *
from final
order by shop_subdomain, dt
