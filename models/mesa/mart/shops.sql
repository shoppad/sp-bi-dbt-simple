{% set columns_to_skip = ['billing', 'entitlements', 'all_shop_ids'] %}

WITH trimmed_shops AS (
    SELECT
        {{ groomed_column_list(ref('stg_shops'), columns_to_skip=columns_to_skip) }}
    FROM {{ ref('stg_shops') }}
),

billing_accounts AS (
    SELECT * FROM {{ ref('stg_mesa_billing_accounts') }}
),

price_per_actions AS (
    SELECT
        shop_id,
        "value" AS price_per_action
    FROM {{ ref('stg_shop_entitlements') }}
    WHERE "name" = 'price_per_action'
),

workflow_counts AS (
    SELECT
        shop_id,
        COUNT(*) AS workflow_count
    FROM {{ ref('workflows') }}
    GROUP BY
        1
),

workflow_run_counts AS (
    SELECT
        shop_id,
        SUM(run_start_count) AS total_workflow_run_starts_count,
        SUM(run_success_count) AS total_workflow_run_success_count
    FROM {{ ref('workflows') }}
    GROUP BY
        1
)

SELECT *
FROM trimmed_shops
LEFT JOIN billing_accounts USING (shop_id)
LEFT JOIN price_per_actions USING (shop_id)
LEFT JOIN workflow_counts USING (shop_id)
LEFT JOIN workflow_run_counts USING (shop_id)
WHERE billing_accounts.billing_plan_name IS NOT NULL
ORDER BY first_installed_at ASC
