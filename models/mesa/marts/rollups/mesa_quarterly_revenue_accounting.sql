WITH
      -- This is qau growth accounting. Note that this does not require any
    -- information about inc_amount. As discussed in the articles, these
    -- quantities satisfy some identities:
    -- qau(t) = retained(t) + new(t) + resurrected(t)
    -- qau(t - 1 quarter) = retained(t) + churned(t)

    qrr_growth_accounting as (
        select
            coalesce(this_q.quarter, dateadd(quarter, 1, last_q.quarter)) as quarter,
            sum(this_q.inc_amount) as rev,
            rev - COALESCE(sum(last_q.inc_amount), 0) AS net_new_rev,
            sum(
                case
                    when this_q.shop_subdomain is not NULL and last_q.shop_subdomain is not NULL
                        and this_q.inc_amount >= last_q.inc_amount then last_q.inc_amount
                    when this_q.shop_subdomain is not NULL and last_q.shop_subdomain is not NULL
                        and this_q.inc_amount < last_q.inc_amount then this_q.inc_amount
                    else 0
                end
            ) as retained,
            sum(
                case when this_q.first_quarter = this_q.quarter then this_q.inc_amount
                else 0 end
            ) as new,
            sum(
                case when this_q.quarter != this_q.first_quarter and this_q.shop_subdomain is not NULL
                    and last_q.shop_subdomain is not NULL and this_q.inc_amount > last_q.inc_amount
                    and last_q.inc_amount > 0 then this_q.inc_amount - last_q.inc_amount
                else 0 end
            ) as expansion,
            sum(
                case when this_q.shop_subdomain is not NULL
                    and (last_q.shop_subdomain is NULL or last_q.inc_amount = 0)
                    and this_q.inc_amount > 0 and this_q.first_quarter != this_q.quarter
                    then this_q.inc_amount
                else 0 end
            ) as resurrected,
            -1 * sum(
                case
                    when this_q.quarter != this_q.first_quarter and this_q.shop_subdomain is not NULL
                        and last_q.shop_subdomain is not NULL
                        and this_q.inc_amount < last_q.inc_amount and this_q.inc_amount > 0
                        then last_q.inc_amount - this_q.inc_amount
                else 0 end
            ) as contraction,
            -1 * sum(
                case when last_q.inc_amount > 0 and (this_q.shop_subdomain is NULL or this_q.inc_amount = 0)
                then last_q.inc_amount else 0 end
            ) as churned,
            (contraction + churned) AS lost_revenue,
            COUNT(DISTINCT this_q.shop_subdomain) AS customer_count,
            COUNT_IF(last_q.shop_subdomain IS NULL) AS new_customer_count,
            COUNT_IF(this_q.shop_subdomain IS NOT NULL AND last_q.shop_subdomain IS NOT NULL) AS retained_customer_count,
            COUNT_IF(
                this_q.shop_subdomain is not NULL
                    and (last_q.shop_subdomain is NULL or last_q.inc_amount = 0)
                    and this_q.inc_amount > 0 and this_q.first_quarter != this_q.quarter
            ) AS resurrected_customer_count,
            COUNT_IF(this_q.shop_subdomain IS NULL) AS churned_customer_count,
            rev * 1.0 / customer_count AS arpu,
            COALESCE(-1 * lost_revenue / sum(last_q.inc_amount), 0) AS revenue_churn_rate,
            arpu * 1.0 / NULLIF(revenue_churn_rate, 0) AS predictive_ltv
        from
            {{ ref('int_shop_quarters') }} AS this_q
            full outer join {{ ref('int_shop_quarters') }} AS last_q on (
                this_q.shop_subdomain = last_q.shop_subdomain
                and this_q.quarter = dateadd(quarter, 1, last_q.quarter)
            )
        group by 1
        order by 1
      )
      -- These next tables are to compute LTV via the cohorts_cumulative table.
    -- The LTV here is being computed for quarterly cohorts on quarterly intervals.
    -- The queries can be modified to compute it for cohorts of any size
    -- on any time window frequency.


select *,
    COALESCE(rev * 1.0 / LAG(rev) OVER (ORDER BY quarter ASC), 0) AS rev_growth_rate,
    COALESCE(churned_customer_count * 1.0 / LAG(customer_count) OVER (ORDER BY quarter ASC), 0) AS customer_churn_rate,
    COALESCE(
        (customer_count * 1.0 / LAG(customer_count) OVER (ORDER BY quarter ASC)) - 1,
        0
    ) AS customer_growth_rate
 from qrr_growth_accounting WHERE quarter <= date_trunc('quarter', current_date()) ORDER BY quarter DESC
