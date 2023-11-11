with
    shops as (
        select shop_subdomain
        from {{ ref("shops") }}
        where is_mql and not is_shopify_zombie_plan
    ),

    calendar as (
        select distinct date_trunc(week, date_day) as dt
        from {{ ref("calendar_dates") }}
    ),

    ends_of_weeks as (select dateadd(day, 6, dt) from calendar),

    shop_days as (
        select shop_subdomain, dt, inc_amount
        from {{ ref("mesa_shop_days") }}
        left join shops using (shop_subdomain)

        where inc_amount > 0

    ),

    first_shop_weeks as (
        select shop_subdomain, date_trunc(week, min(dt)) as cohort_week
        from shop_days
        group by 1
    ),

    decorated_first_shop_weeks as (
        select shop_subdomain, cohort_week, avg(inc_amount) * 30 as mrr
        from shop_days
        left join first_shop_weeks using (shop_subdomain)
        where
            date_part(year, cohort_week) = date_part(year, dt)
            and date_part(week, cohort_week) = date_part(week, dt)
        group by 1, 2
    ),

    cohort_info as (
        select
            cohort_week,
            count(distinct shop_subdomain) as cohort_size,
            round(sum(mrr)) as cohort_starting_mrr
        from decorated_first_shop_weeks
        group by 1
    ),

    shop_weeks as (
        select distinct
            date_trunc(week, dt) as period_week,
            shop_subdomain,
            first_shop_weeks.cohort_week,
            inc_amount * 30 as week_mrr

        from shop_days
        left join first_shop_weeks using (shop_subdomain)
        where dt in (select * from ends_of_weeks)
    ),

    shop_week_activity as (
        select
            cohort_week,
            period_week,
            count(distinct shop_subdomain) as retained_shops,
            sum(week_mrr) as retained_mrr
        from shop_weeks
        group by 1, 2
    )

select
    cohort_week,
    concat(
        cohort_week,
        ' [',
        cohort_size,
        ' / ',
        to_varchar(cohort_starting_mrr, '$9,000'),
        ']'
    ) as cohort_info,
    floor(datediff(week, cohort_week, period_week)) as period,
    retained_shops,
    retained_shops / cohort_size::float as retention_rate,
    retained_mrr,
    retained_mrr / cohort_starting_mrr as revenue_retention_rate
from cohort_info

left join shop_week_activity using (cohort_week)
where period is not null and cohort_week > current_date - interval '60 weeks'
order by 1, 2
