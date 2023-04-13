WITH
-- This is MAU growth accounting. Note that this does not require any
-- information about inc_amount. As discussed in the articles, these
-- quantities satisfy some identities:
-- MAU(t) = retained(t) + new(t) + resurrected(t)
-- MAU(t - 1 month) = retained(t) + churned(t)
-- monthly cumulative cohorts

cohorts_m AS (
    SELECT
        first_month,
        month AS active_month,
        datediff(month, first_month, month) AS months_since_first,
        count(DISTINCT shop_subdomain) AS shops,
        sum(inc_amount) AS inc_amt
    FROM {{ ref('int_shop_months') }}
    WHERE inc_amount > 0
    GROUP BY 1, 2, 3
    ORDER BY 1, 2
),

cohort_sizes_m AS (
    SELECT
        first_month,
        shops,
        inc_amt
    FROM cohorts_m
    WHERE months_since_first = 0
),

cohorts_cumulative_m AS (
    -- A semi-cartesian join accomplishes the cumulative behavior.
    SELECT
        c1.first_month,
        c1.active_month,
        c1.months_since_first,
        c1.shops,
        cs.shops as cohort_num_users,
        1.0 * c1.shops / cs.shops as retained_pctg,
        c1.inc_amt,
        sum(c2.inc_amt) as cum_amt,
        1.0 * sum(c2.inc_amt) / cs.shops as cum_amt_per_user
    FROM
        cohorts_m AS c1,
        cohorts_m AS c2,
        cohort_sizes_m AS cs
    WHERE
        c1.first_month = c2.first_month
        AND c2.months_since_first <= c1.months_since_first
        AND cs.first_month = c1.first_month
    GROUP BY
        1, 2, 3, 4, 5, 6, 7
    ORDER BY
        1, 2
)

-- For MAU growth accounting use this
SELECT *
FROM cohorts_cumulative_m
WHERE active_month <= date_trunc('month', CURRENT_DATE())
