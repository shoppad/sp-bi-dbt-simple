WITH final AS (
    SELECT
        shop_subdomain,
        billing:method.name::STRING AS billing_method_name,
        {{ pacific_timestamp('to_timestamp(billing:overage.bucket_start::INT, 3)') }} AS billing_overage_bucket_start_date_pt,
        {{ pacific_timestamp('to_timestamp(billing:overage.bucket_end::INT, 3)') }} AS billing_overage_bucket_end_date_pt,
        billing:overage.bypass_until AS billing_overage_bypass_until,
        billing:overage.last_count::FLOAT AS billing_overage_last_count,
        billing:plan.days_complete::FLOAT AS billing_plan_days_complete,
        billing:plan.id::STRING AS billing_plan_id,
        billing:plan.percent_complete::FLOAT AS billing_plan_percent_complete,
        billing:plan.percent_used::FLOAT AS billing_plan_percent_used,

        {% set plan_start_date -%}
            to_timestamp(
                CASE
                    WHEN IS_NULL_VALUE(billing:plan:start) OR billing:plan:start IS NULL
                        THEN billing:plan_start::INT
                    ELSE billing:plan:start::INT
                END, 3)
        {%- endset -%}
        {{ pacific_timestamp(plan_start_date) }} AS billing_plan_start_date_pt,

        {% set plan_end_date -%}
            to_timestamp(
                CASE
                    WHEN IS_NULL_VALUE(billing:plan:end) OR billing:plan:end IS NULL
                        THEN billing:plan_end::INT
                    ELSE billing:plan:end::INT
                END, 3)
        {%- endset -%}

        {{ pacific_timestamp(plan_end_date) }} AS billing_plan_end_date_pt,
        billing:plan.status::STRING AS billing_plan_status,
        billing:plan.used::FLOAT AS billing_plan_used,
        billing:plan_name::STRING AS billing_plan_name,
        billing:method.shopify_id::FLOAT AS billing_method_shopify_id,
        billing:plan.trial_days::FLOAT AS billing_plan_trial_days,
        {{ pacific_timestamp('to_timestamp(billing:plan.trial_ends::INT, 3)') }} AS billing_plan_trial_ends_pt,
        {{ pacific_timestamp('billing:plan.updated_at::STRING') }} AS billing_plan_updated_at_pt,
        billing:plan.billing_on::STRING AS billing_plan_billing_on_pt,
        billing:plan.overlimit_date::STRING AS billing_plan_overlimit_date_pt,
        billing:plan.balance_remaining::STRING AS billing_plan_balance_remaining,
        billing:method.chargebee_id::STRING AS billing_method_chargebee_id,
        billing:plan_volume::FLOAT AS billing_plan_volume,
        CASE
            WHEN IS_NULL_VALUE(billing:plan.interval) OR billing:plan.interval IS NULL
                THEN billing:plan_interval
            ELSE billing:plan.interval
        END AS billing_plan_interval,
        billing:plan_price::STRING AS billing_plan_price,
        {{ pacific_timestamp('billing:plan.created_at::STRING') }} AS billing_plan_created_at_pt,
        billing:plan.balance_used::STRING AS billing_plan_balance_used,
        billing:plan_type::STRING AS billing_plan_type,
        IFF(billing_plan_price = '', '0', IFF(billing_plan_interval = 'annual', billing_plan_price / 365, billing_plan_price / 30)) AS daily_plan_revenue
    FROM {{ ref('stg_shops') }}
)

SELECT * FROM final
