WITH final AS (
    SELECT
        shop_id,
        billing:"method"."name"::STRING AS billing_method_name,
        to_timestamp(billing:"overage"."bucket_end"::INT, 3) AS billing_overage_bucket_end,
        to_timestamp(billing:"overage"."bucket_start"::INT, 3) AS billing_overage_bucket_start,
        billing:"overage"."bypass_until"::STRING AS billing_overage_bypass_until,
        billing:"overage"."last_count"::FLOAT AS billing_overage_last_count,
        billing:"plan"."days_complete"::FLOAT AS billing_plan_days_complete,
        billing:"plan"."id"::STRING AS billing_plan_id,
        billing:"plan"."percent_complete"::FLOAT AS billing_plan_percent_complete,
        billing:"plan"."percent_used"::FLOAT AS billing_plan_percent_used,
        to_timestamp(COALESCE(billing:"plan"."start"::INT, billing:"plan_start"::INT), 3) AS billing_plan_start,
        billing:"plan"."status"::STRING AS billing_plan_status,
        billing:"plan"."used"::FLOAT AS billing_plan_used,
        billing:"plan_name"::STRING AS billing_plan_name,
        billing:"method"."shopify_id"::FLOAT AS billing_method_shopify_id,
        billing:"plan"."trial_days"::FLOAT AS billing_plan_trial_days,
        to_timestamp(billing:"plan"."trial_ends"::INT, 3) AS billing_plan_trial_ends,
        {{ pacific_timestamp('billing:"plan"."updated_at"::STRING') }} AS billing_plan_updated_at_pt,
        billing:"plan"."billing_on"::STRING AS billing_plan_billing_on,
        {{ pacific_timestamp('to_timestamp(COALESCE(billing:"plan"."end"::INT, billing:"plan_end"::INT), 3)') }} AS billing_plan_end_at_pt,
        billing:"plan"."overlimit_date"::STRING AS billing_plan_overlimit_date,
        billing:"plan"."balance_remaining"::STRING AS billing_plan_balance_remaining,
        billing:"method"."chargebee_id"::STRING AS billing_method_chargebee_id,
        billing:"plan_volume"::FLOAT AS billing_plan_volume,
        COALESCE(billing:"plan"."interval"::STRING, billing:"plan_interval"::STRING) AS billing_plan_interval,
        billing:"plan_price"::STRING AS billing_plan_price,
        {{ pacific_timestamp('billing:"plan"."created_at"::STRING') }} AS billing_plan_created_at_pt,
        billing:"plan"."balance_used"::STRING AS billing_plan_balance_used,
        billing:"plan_type"::STRING AS billing_plan_type,
        IFF(billing_plan_price = '', '0', IFF(billing_plan_interval = 'annual', billing_plan_price/365, billing_plan_price/30)) AS daily_plan_revenue
    FROM {{ ref('stg_shops') }}
)

SELECT * FROM final
