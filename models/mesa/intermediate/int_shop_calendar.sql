with
    shops as (select shop_subdomain, is_custom_app from {{ ref("stg_shops") }}),

    shop_plan_days as (select * from {{ ref("int_mesa_shop_plan_days") }}),

    shop_lifespans as (select * from {{ ref("int_shop_lifespans") }}),

    calendar_dates as (
        select date_day as dt
        from {{ ref("calendar_dates") }}
        where
            dt <= {{ pacific_timestamp("CURRENT_TIMESTAMP") }}::date - interval '1 DAY'
    ),

    shop_trial_days as (
        select shop_subdomain, dt, true as is_in_trial
        from calendar_dates
        inner join
            {{ ref("stg_trial_periods") }}
            on calendar_dates.dt between started_on_pt and coalesce(
                ended_on_pt, {{ pacific_timestamp("CURRENT_DATE") }}::date
            )
    ),

    shop_calendar as (
        select
            shop_subdomain,
            dt,
            coalesce(
                iff(
                    is_shopify_zombie_plan or coalesce(is_in_trial, false),
                    0,
                    daily_plan_revenue
                ),
                0
            ) as daily_plan_revenue,
            mesa_plan,
            mesa_plan_identifier,
            coalesce(shopify_plan, 'unavailable') as shopify_plan,
            coalesce(shop_trial_days.is_in_trial, false) as is_in_trial,
            coalesce(is_shopify_zombie_plan, false) as is_shopify_zombie_plan

        from shop_lifespans
        inner join calendar_dates on dt between first_dt and last_dt
        left join shop_plan_days using (shop_subdomain, dt)
        left join shops using (shop_subdomain)
        left join shop_trial_days using (shop_subdomain, dt)
        order by dt
    )

select *
from shop_calendar
order by shop_subdomain, dt
