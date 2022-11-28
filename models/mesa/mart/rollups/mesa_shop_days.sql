WITH

shops AS (
    SELECT
        shop_subdomain
    FROM {{ ref('int_shops') }}
),

billing_accounts AS (
    SELECT
        shop_subdomain,
        COALESCE(daily_plan_revenue, 0) AS daily_plan_revenue
    FROM shops
    LEFT JOIN {{ ref('stg_mesa_billing_accounts') }} USING (shop_subdomain)
    {# ?: Is it important to include this? Won't charges never appear if they are still in Trial?
        (
            billing_plan_trial_ends IS NULL OR
            billing_plan_trial_ends < current_date()
        ) #}
),

shop_days AS (
    SELECT *
    FROM {{ ref('int_mesa_shop_days') }}
),

final AS (
    SELECT
        *,
        daily_plan_revenue + daily_usage_revenue AS inc_amount
    FROM shop_days
    LEFT JOIN billing_accounts USING (shop_subdomain)
)

SELECT * FROM final
