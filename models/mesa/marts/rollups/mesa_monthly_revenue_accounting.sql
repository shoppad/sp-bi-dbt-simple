WITH
      -- This is MAU growth accounting. Note that this does not require any
    -- information about inc_amount. As discussed in the articles, these
    -- quantities satisfy some identities:
    -- MAU(t) = retained(t) + new(t) + resurrected(t)
    -- MAU(t - 1 month) = retained(t) + churned(t)

    mrr_growth_accounting as (
        select
            coalesce(tm.month, dateadd(month,1,lm.month)) as month,
            sum(tm.inc_amount) as rev,
            sum(
                case
                    when tm.shop_subdomain is not NULL and lm.shop_subdomain is not NULL
                        and tm.inc_amount >= lm.inc_amount then lm.inc_amount
                    when tm.shop_subdomain is not NULL and lm.shop_subdomain is not NULL
                        and tm.inc_amount < lm.inc_amount then tm.inc_amount
                    else 0
                end
            ) as retained,
            sum(
                case when tm.first_month = tm.month then tm.inc_amount
                else 0 end
            ) as new,
            sum(
                case when tm.month != tm.first_month and tm.shop_subdomain is not NULL
                    and lm.shop_subdomain is not NULL and tm.inc_amount > lm.inc_amount
                    and lm.inc_amount > 0 then tm.inc_amount - lm.inc_amount
                else 0 end
            ) as expansion,
            sum(
                case when tm.shop_subdomain is not NULL
                    and (lm.shop_subdomain is NULL or lm.inc_amount = 0)
                    and tm.inc_amount > 0 and tm.first_month != tm.month
                    then tm.inc_amount
                else 0 end
            ) as resurrected,
            -1 * sum(
                case
                    when tm.month != tm.first_month and tm.shop_subdomain is not NULL
                        and lm.shop_subdomain is not NULL
                        and tm.inc_amount < lm.inc_amount and tm.inc_amount > 0
                        then lm.inc_amount - tm.inc_amount
                else 0 end
            ) as contraction,
            -1 * sum(
                case when lm.inc_amount > 0 and (tm.shop_subdomain is NULL or tm.inc_amount = 0)
                then lm.inc_amount else 0 end
            ) as churned
        from
            {{ ref('int_shop_months') }} tm
            full outer join {{ ref('int_shop_months') }} lm on (
                tm.shop_subdomain = lm.shop_subdomain
                and tm.month = dateadd(month,1,lm.month)
            )
        group by 1
        order by 1
      )
      -- These next tables are to compute LTV via the cohorts_cumulative table.
    -- The LTV here is being computed for weekly cohorts on weekly intervals.
    -- The queries can be modified to compute it for cohorts of any size
    -- on any time window frequency.

-- For MRR growth accuonting use this
select * from mrr_growth_accounting WHERE month < date_trunc('month', current_date()) ORDER BY month DESC
