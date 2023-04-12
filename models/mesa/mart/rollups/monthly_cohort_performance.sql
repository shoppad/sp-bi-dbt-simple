WITH
-- This is MAU growth accounting. Note that this does not require any
-- information about inc_amount. As discussed in the articles, these
-- quantities satisfy some identities:
-- MAU(t) = retained(t) + new(t) + resurrected(t)
-- MAU(t - 1 month) = retained(t) + churned(t)
 -- monthly cumulative cohorts
    cohorts_m as (
        select
            first_month,
            month as active_month,
            datediff(month, first_month, month) as months_since_first,
            count(distinct shop_subdomain) as users,
            sum(inc_amount) as inc_amt
        from {{ ref('int_shop_months') }}
        group by 1,2,3
        order by 1,2
    ),
cohort_sizes_m as (
    select
        first_month,
        users,
        inc_amt
    from cohorts_m
    where months_since_first = 0
),
    cohorts_cumulative_m as (
        -- A semi-cartesian join accomplishes the cumulative behavior.
        select
            c1.first_month,
            c1.active_month,
            c1.months_since_first,
            c1.users,
            cs.users as cohort_num_users,
            1.0 * c1.users/cs.users as retained_pctg,
            c1.inc_amt,
            sum(c2.inc_amt) as cum_amt,
            1.0*sum(c2.inc_amt)/cs.users as cum_amt_per_user
        from
            cohorts_m c1,
            cohorts_m c2,
            cohort_sizes_m cs
        where
            c1.first_month = c2.first_month
            and c2.months_since_first <= c1.months_since_first
            and cs.first_month = c1.first_month
        group by 1,2,3,4,5,6,7
        order by 1, 2
    )

-- For MAU growth accounting use this
SELECT
    *
FROM cohorts_cumulative_m
WHERE active_month <= date_trunc('month', CURRENT_DATE())

