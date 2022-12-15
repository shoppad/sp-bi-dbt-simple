WITH
-- This is MAU growth accounting. Note that this does not require any
-- information about inc_amount. As discussed in the articles, these
-- quantities satisfy some identities:
-- MAU(t) = retained(t) + new(t) + resurrected(t)
-- MAU(t - 1 month) = retained(t) + churned(t)
mau_growth_accounting AS (
    SELECT
        coalesce(this_month.month, dateadd(month, 1, last_month.month))::DATE AS month,
        count(distinct this_month.shop_subdomain) AS mau,
        count(distinct CASE WHEN last_month.shop_subdomain is not NULL THEN this_month.shop_subdomain END) AS retained,
        count(distinct CASE WHEN this_month.first_month = this_month.month THEN this_month.shop_subdomain END) AS new,
        count(distinct CASE WHEN this_month.first_month != this_month.month
                AND last_month.shop_subdomain IS NULL THEN this_month.shop_subdomain END
        ) AS resurrected,
        -1 * count(distinct CASE WHEN this_month.shop_subdomain is NULL THEN last_month.shop_subdomain END) AS churned
    FROM
        {{ ref('int_shop_months') }} AS this_month
    FULL OUTER JOIN {{ ref('int_shop_months') }} AS last_month ON (
        this_month.shop_subdomain = last_month.shop_subdomain
        and this_month.month = dateadd(month, 1, last_month.month)
        )
    GROUP BY 1
)

-- For MAU growth accounting use this
SELECT *
FROM mau_growth_accounting
WHERE month <= date_trunc('month', CURRENT_DATE())
ORDER BY month DESC
