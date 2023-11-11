{% set source_table = ref("stg_shops") %}
with
    decorated_shops as (
        {% set columns_to_skip = [
            "scopes",
            "billing",
            "status",
            "entitlements",
            "timestamp",
            "shopify",
            "usage",
            "config",
            "themes",
            "webhooks",
            "messages",
            "analytics",
            "schema",
            "handle",
            "method",
            "account",
            "wizard",
            "mongoid",
            "authtoken",
            "metabase",
        ] %}
        select
            {{
                groomed_column_list(source_table, except=columns_to_skip) | join(
                    ",\n       "
                )
            }},
            shopify:currency::string as currency,
            {{ pacific_timestamp("cast(shopify:created_at AS TIMESTAMP_LTZ)") }}
            as shopify_shop_created_at_pt,
            shopify:country::string as shopify_shop_country,
            status as install_status,
            analytics:initial:orders_count::numeric
            as shopify_shop_orders_initial_count,
            analytics:initial:orders_gmv::numeric as shopify_shop_gmv_initial_total,
            analytics:orders:count::numeric as shopify_shop_orders_current_count,
            analytics:orders:gmv::numeric as shopify_shop_gmv_current_total,
            analytics:initial:shopify_plan_name::string as initial_shopify_plan_name,
            coalesce(
                wizard:builder:step = 'complete', false
            ) as is_builder_wizard_completed,
            {{ datediff("shopify_shop_created_at_pt", "first_installed_at_pt", "day") }}
            as age_of_store_at_install_in_days,
            {{
                datediff(
                    "shopify_shop_created_at_pt", "first_installed_at_pt", "week"
                )
            }} as age_of_store_at_install_in_weeks,
            case
                when age_of_store_at_install_in_days = 0
                then '1-First Day'
                when age_of_store_at_install_in_days <= 7
                then '2-First Week (Day 2-7)'
                when age_of_store_at_install_in_days <= 31
                then '3-First Month (After First Week)'
                when age_of_store_at_install_in_days <= 90
                then '4-First Quarter (After First Month)'
                when age_of_store_at_install_in_days <= 180
                then '5-First Half (After First Quarter)'
                when age_of_store_at_install_in_days <= 365
                then '6-First Year (After First Half)'
                when age_of_store_at_install_in_days <= 547
                then '7-First 18 Months (After First Year)'
                when age_of_store_at_install_in_days <= 730
                then '8-First 2 Years (After 18 Months)'
                else '9-2nd Year+'
            end as age_of_store_at_install_bucket
        from {{ source_table }}
    ),

activation_dates AS (
    SELECT
        shop_subdomain,
        MIN(dt) AS activation_date_pt
    FROM {{ ref('int_mesa_shop_days') }}
    WHERE is_active
    GROUP BY
        1
),

launch_session_dates AS (
    SELECT
        shop_subdomain,
        IFF(
            meta_attribs.value:name = 'launchsessiondate',
            meta_attribs.value:value::DATE,
            NULL
        ) AS launch_session_date,
        NOT launch_session_date IS NULL AS has_had_launch_session
    FROM {{ ref('stg_shops') }},
        LATERAL FLATTEN(input => meta) AS meta_attribs
),

plan_upgrade_dates AS (
    SELECT
        shop_subdomain,
        MIN(dt) AS first_plan_upgrade_date,
        MIN_BY(mesa_plan_identifier, dt) AS first_plan_identifier,
        COUNT_IF(inc_amount > 0) AS paid_days_completed,
        paid_days_completed > 0 AS has_ever_upgraded_to_paid_plan
    FROM decorated_shops
    LEFT JOIN (SELECT * FROM {{ ref('int_mesa_shop_days') }} WHERE inc_amount > 0) USING (shop_subdomain)
    GROUP BY 1
),

trial_dates AS (
    SELECT
        shop_subdomain,
        MIN(dt) AS first_trial_start_date,
        MIN_BY(mesa_plan_identifier, dt) AS first_trial_plan_identifier,
        COUNT_IF(is_in_trial) AS trial_days_completed,
        trial_days_completed > 0 AS has_done_a_trial
    FROM decorated_shops
    LEFT JOIN (SELECT * FROM {{ ref('int_mesa_shop_days') }} WHERE is_in_trial) USING (shop_subdomain)

    GROUP BY 1
),

conversion_rates AS (
    SELECT
        currency,
        in_usd
    FROM {{ ref('currency_conversion_rates') }}
),

final AS (
    SELECT
        * EXCLUDE (shopify_shop_gmv_current_total, shopify_shop_gmv_initial_total, in_usd),
        1.0 * shopify_shop_gmv_initial_total * in_usd AS shopify_shop_gmv_initial_total_usd,
        1.0 * shopify_shop_gmv_current_total * in_usd AS shopify_shop_gmv_current_total_usd,
        COALESCE(in_usd IS NULL, FALSE) AS currency_not_supported,
        first_trial_start_date - first_installed_on_pt AS days_until_first_trial,
        first_plan_upgrade_date - first_installed_on_pt AS days_until_first_plan_upgrade
    FROM decorated_shops
    LEFT JOIN activation_dates USING (shop_subdomain)
    LEFT JOIN launch_session_dates USING (shop_subdomain)
    LEFT JOIN conversion_rates USING (currency)
    LEFT JOIN plan_upgrade_dates USING (shop_subdomain)
    LEFT JOIN trial_dates USING (shop_subdomain)

)

SELECT * FROM final
