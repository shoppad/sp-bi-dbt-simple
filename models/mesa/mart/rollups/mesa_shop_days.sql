WITH charges AS (
    SELECT *
    FROM {{ ref('mesa_charges') }}
    WHERE charged_on_pt < current_date()
),

billing_accounts AS (
    SELECT
        shop_id,
        daily_plan_revenue
    FROM {{ ref('stg_mesa_billing_accounts') }}
    {# Is it important to include this? Won't charges never appear if they are still in Trial?
        (
            billing_plan_trial_ends IS NULL OR
            billing_plan_trial_ends < current_date()
        ) #}

),

shops AS (
    SELECT *
    FROM {{ ref('shops') }}
    WHERE install_status = 'active'
        AND shopify_plan_name NOT IN ('frozen', 'cancelled', 'fraudulent')
)

SELECT
    charges.charged_on_pt,
    shop_id,
    shop_subdomain,
    daily_plan_revenue,
    COALESCE(charges.billed_amount, 0) as daily_usage_revenue,
    (daily_plan_revenue + daily_usage_revenue) as inc_amount
FROM charges
LEFT JOIN shops USING (shop_id)
LEFT JOIN billing_accounts USING (shop_id)
-- Don't create rows for zero amounts.
{# WHERE inc_amount > 0 -- This is handled in the Growth Accounting queries. #}
