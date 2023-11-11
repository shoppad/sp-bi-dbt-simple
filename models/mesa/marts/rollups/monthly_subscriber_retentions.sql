with

    shops as (
        select shop_subdomain
        from {{ ref("shops") }}
        where is_mql and not is_shopify_zombie_plan
    ),

    calendar as (
        select distinct date_trunc(month, date_day) as dt
        from {{ ref("calendar_dates") }}
    ),

    ends_of_months as (select distinct last_day(dt) as dt from calendar),

    shop_days as (
        select shop_subdomain, dt, inc_amount
        from {{ ref("mesa_shop_days") }}
        inner join shops using (shop_subdomain)
        where inc_amount > 0
    ),

    first_shop_months as (
        select shop_subdomain, date_trunc(month, min(dt)) as cohort_month
        from shop_days
        group by 1
    ),

    decorated_first_shop_months as (
        select shop_subdomain, cohort_month, avg(inc_amount) * 30 as mrr
        from shop_days
        left join first_shop_months using (shop_subdomain)
        where
            date_part(year, cohort_month) = date_part(year, dt)
            and date_part(month, cohort_month) = date_part(month, dt)
        group by 1, 2
    ),

    cohort_info as (
        select
            cohort_month,
            count(distinct shop_subdomain) as cohort_size,
            round(sum(mrr)) as cohort_starting_mrr
        from decorated_first_shop_months
        group by 1
    ),

    shop_months as (
        select distinct
            date_trunc(month, dt) as period_month,
            shop_subdomain,
            first_shop_months.cohort_month,
            inc_amount * 30 as month_mrr

        from shop_days
        left join first_shop_months using (shop_subdomain)
        where dt in (select * from ends_of_months)
    ),

    shop_month_activity as (
        select
            cohort_month,
            period_month,
            count(distinct shop_subdomain) as retained_shops,
            sum(month_mrr) as retained_mrr
        from shop_months
        group by 1, 2
    )

select
    cohort_month,
    concat(
        cohort_month,
        ' [',
        cohort_size,
        ' / ',
        to_varchar(cohort_starting_mrr, '$9,000'),
        ']'
    ) as cohort_info,
    floor(datediff(month, cohort_month, period_month)) as period,
    retained_shops,
    retained_shops / cohort_size::float as retention_rate,
    retained_mrr,
    retained_mrr / cohort_starting_mrr as revenue_retention_rate
from cohort_info

left join shop_month_activity using (cohort_month)
where period is not null
order by 1, 2
