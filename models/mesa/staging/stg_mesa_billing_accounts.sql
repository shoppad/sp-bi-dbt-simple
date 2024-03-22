WITH
    shops AS (SELECT shop_subdomain, billing FROM {{ ref("stg_shops") }}),

    final AS (
        SELECT
            shop_subdomain,
            COALESCE(billing:method:name, 'shopify')::STRING AS billing_method_name,
            COALESCE(
                billing:overage:last_count::FLOAT, 0
            ) AS billing_overage_last_count,
            billing:plan:days_complete::FLOAT AS days_complete,
            billing:plan:id::STRING AS plan_id,
            billing:plan:percent_complete::FLOAT AS percent_complete,
            billing:plan:percent_used::FLOAT AS percent_used,

            billing:plan:status::STRING AS status,
            billing:plan:used::FLOAT AS plan_used,
            billing:plan_name::STRING AS plan_name,
            billing:plan:trial_days::FLOAT AS trial_days,
            {{ pacific_timestamp("to_timestamp(billing:plan:trial_ends::INT, 3)") }}
                AS trial_ends_pt,
            {{ pacific_timestamp("billing:plan:updated_at::STRING") }}
                AS billing_updated_at_pt,
            IFF(
                IS_NULL_VALUE(billing:plan:billing_on) OR billing:plan:billing_on IS NULL,
                NULL,
                TO_TIMESTAMP_NTZ(billing:plan:billing_on::VARCHAR)
            ) AS billing_on_pt,
            billing:plan:overlimit_date::STRING AS overlimit_date_pt,
            billing:plan:balance_remaining::NUMERIC AS balance_remaining,
            billing:method:chargebee_id::STRING AS chargebee_id,
            CASE
                WHEN
                    is_null_value(billing:plan:interval)
                    OR billing:plan:interval IS NULL
                THEN billing:plan_interval
                ELSE billing:plan:interval
            END::STRING AS plan_interval,
            IFF(billing:plan_price = '', NULL, billing:plan_price)::NUMERIC AS plan_price,
            IFF(billing:plan:balance_used = '', NULL, billing:plan:balance_used)::NUMERIC AS balance_used,
            billing:plan_type::STRING AS plan_type,


            CAST(
                IFF(
                    plan_interval = 'annual',
                    plan_price / 365::FLOAT,
                    plan_price / 30::FLOAT
                ) AS NUMBER(38, 20)
            )
            AS daily_plan_revenue
        FROM shops
    )

SELECT *
FROM final
