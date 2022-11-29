WITH
shops AS (
    SELECT
        shop_subdomain,
        billing
    FROM {{ ref('stg_shops') }}
),

final AS (
    SELECT
        shop_subdomain,
        COALESCE(billing:method:name, 'shopify')::STRING AS billing_method_name,
        {{ pacific_timestamp('to_timestamp(billing:overage:bucket_start::INT, 3)') }} AS billing_overage_bucket_start_date_pt,
        {{ pacific_timestamp('to_timestamp(billing:overage:bucket_end::INT, 3)') }} AS billing_overage_bucket_end_date_pt,
        IFF(IS_NULL_VALUE(billing:overage:bypass_until), NULL, billing:overage:bypass_until) AS billing_overage_bypass_until,
        billing:overage:last_count::FLOAT AS billing_overage_last_count,
        billing:plan:days_complete::FLOAT AS days_complete,
        billing:plan:id::STRING AS plan_id,
        billing:plan:percent_complete::FLOAT AS percent_complete,
        billing:plan:percent_used::FLOAT AS percent_used,

        {% set plan_start_date -%}
            to_timestamp(
                CASE
                    WHEN IS_NULL_VALUE(billing:plan:start) OR billing:plan:start IS NULL
                        THEN billing:plan_start::INT
                    ELSE billing:plan:start::INT
                END, 3)
        {%- endset -%}
        {{ pacific_timestamp(plan_start_date) }} AS start_date_pt,

        {% set plan_end_date -%}
            to_timestamp(
                CASE
                    WHEN IS_NULL_VALUE(billing:plan:end) OR billing:plan:end IS NULL
                        THEN billing:plan_end::INT
                    ELSE billing:plan:end::INT
                END, 3)
        {%- endset -%}

        {{ pacific_timestamp(plan_end_date) }} AS end_date_pt,
        billing:plan:status::STRING AS status,
        billing:plan:used::FLOAT AS plan_used,
        billing:plan_name::STRING AS plan_name,
        billing:method:shopify_id::FLOAT AS shopify_id,
        billing:plan:trial_days::FLOAT AS trial_days,
        {{ pacific_timestamp('to_timestamp(billing:plan:trial_ends::INT, 3)') }} AS trial_ends_pt,
        {{ pacific_timestamp('billing:plan:updated_at::STRING') }} AS billing_updated_at_pt,
        billing:plan:billing_on::STRING AS billing_on_pt,
        billing:plan:overlimit_date::STRING AS overlimit_date_pt,
        billing:plan:balance_remaining::STRING AS balance_remaining,
        billing:method:chargebee_id::STRING AS chargebee_id,
        CASE
            WHEN IS_NULL_VALUE(billing:plan:interval) OR billing:plan:interval IS NULL
                THEN billing:plan_interval
            ELSE billing:plan:interval
        END::STRING AS plan_interval,
        billing:plan_price::STRING AS plan_price,
        {{ pacific_timestamp('billing:plan:created_at::STRING') }} AS created_at_pt,
        billing:plan:balance_used::STRING AS balance_used,
        billing:plan_type::STRING AS plan_type,
        IFF(plan_price = '', '0', IFF(plan_interval = 'annual', plan_price / 365, plan_price / 30)) AS daily_plan_revenue
    FROM shops
)

SELECT * FROM final
