WITH
shops AS (
    SELECT *
    FROM {{ ref('int_shops') }}
),

billing_accounts AS (
    SELECT * FROM {{ ref('stg_mesa_billing_accounts') }}
),

price_per_actions AS (
    SELECT
        shop_subdomain,
        "value" AS price_per_action
    FROM {{ ref('stg_shop_entitlements') }}
    WHERE "name" = 'price_per_action'
),

workflow_counts AS (
    SELECT
        shop_subdomain,
        COUNT(*) AS workflows_created_count,
        COUNT_IF(is_enabled) AS workflows_enabled_count,
        COUNT_IF(first_successful_run_at_pt IS NOT NULL) AS workflows_successfully_run_count,
        COUNT(DISTINCT template_name) AS templates_installed_count
    FROM shops
    LEFT JOIN {{ ref('workflows') }} USING (shop_subdomain)
    GROUP BY
        1
),

workflow_run_counts AS (
    SELECT
        shop_subdomain,
        SUM(run_start_count) AS total_trigger_runs_count,
        SUM(run_success_count) AS total_successful_workflow_run_count
    FROM {{ ref('workflows') }}
    GROUP BY
        1
),

activation_dates AS (
    SELECT
        shop_subdomain,
        MIN(dt) AS activation_date_pt
    FROM mesa_shop_days
    WHERE is_active
    GROUP BY
        1
),

final AS (
    SELECT
        *,
        NOT(activation_date_pt IS NULL) AS is_activated,
        CONCAT('https://www.theshoppad.com/homeroom.theshoppad.com/admin/backdoor/', shop_subdomain, '/mesa') AS backdoor_url,
        CONCAT('https://insights.hotjar.com/sites/1547357/workspaces/1288874/playbacks/list?filters=%7B%22AND%22:%5B%7B%22DAYS_AGO%22:%7B%22created%22:365%7D%7D,%7B%22EQUAL%22:%7B%22user_attributes.str.user_id%22:%22', shop_subdomain, '%22%7D%7D%5D%7D') AS hotjar_url
    FROM shops
    LEFT JOIN billing_accounts USING (shop_subdomain)
    LEFT JOIN price_per_actions USING (shop_subdomain)
    LEFT JOIN workflow_counts USING (shop_subdomain)
    LEFT JOIN workflow_run_counts USING (shop_subdomain)
    LEFT JOIN activation_dates USING (shop_subdomain)
    WHERE billing_accounts.plan_name IS NOT NULL
)

SELECT * FROM final
