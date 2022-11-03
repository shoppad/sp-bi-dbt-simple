{% set shop_columns_to_skip = ['billing', 'entitlements', 'all_shop_ids'] %}

WITH
trimmed_shops AS (
    SELECT
        {{ groomed_column_list(ref('stg_shops'), except=shop_columns_to_skip) | join(",\n        ") }}
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
        shop_id
),

workflow_run_counts AS (
    SELECT
        shop_id,
        SUM(run_start_count) AS total_trigger_runs_count,
        SUM(run_success_count) AS total_successful_workflow_run_count
    FROM {{ ref('workflows') }}
    GROUP BY
        shop_id
),

activation_dates AS (
    SELECT
        shop_id,
        MIN(dt) AS activation_date
    FROM mesa_shop_days
    WHERE is_active
    GROUP BY
        shop_id
),

final AS (
    SELECT
        *,
        activation_date AS activation_date_pt,
        NOT(activation_date IS NULL) AS is_activated
    FROM trimmed_shops
    LEFT JOIN billing_accounts USING (shop_id)
    LEFT JOIN price_per_actions USING (shop_id)
    LEFT JOIN workflow_counts USING (shop_id)
    LEFT JOIN workflow_run_counts USING (shop_id)
    LEFT JOIN activation_dates USING (shop_id)
    WHERE billing_accounts.billing_plan_name IS NOT NULL
)

SELECT * FROM final
