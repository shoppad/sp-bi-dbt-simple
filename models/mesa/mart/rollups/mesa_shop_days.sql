WITH

billing_accounts AS (
    SELECT
        shop_subdomain,
        daily_plan_revenue
    FROM {{ ref('stg_mesa_billing_accounts') }}
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
        (daily_plan_revenue + daily_usage_revenue) AS inc_amount
    FROM shop_days
    LEFT JOIN billing_accounts USING (shop_subdomain)
)

SELECT * FROM final
