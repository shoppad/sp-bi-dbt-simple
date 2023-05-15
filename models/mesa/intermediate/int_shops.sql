{% set source_table = ref('stg_shops') %}
WITH
decorated_shops AS (
    {% set columns_to_skip = ['scopes', 'billing', 'status', 'entitlements', 'timestamp', 'shopify', 'usage', 'config', 'themes', 'webhooks', 'messages', 'analytics', 'schema', 'handle', 'method', 'account', 'wizard', 'mongoid', 'authtoken', 'metabase'] %}
    SELECT
        {{ groomed_column_list(source_table, except=columns_to_skip)  | join(",\n       ") }},
        shopify:plan_name::STRING AS shopify_plan_name,
        shopify:currency::STRING AS currency,
        {{ pacific_timestamp('cast(shopify:created_at AS TIMESTAMP_LTZ)') }} AS shopify_shop_created_at_pt,
        shopify:country::STRING AS shopify_shop_country,
        status AS install_status,
        analytics:initial:orders_count::NUMERIC AS shopify_shop_orders_initial_count,
        analytics:initial:orders_gmv AS shopify_shop_gmv_initial_total,
        analytics:orders:count::NUMERIC AS shopify_shop_orders_current_count,
        analytics:orders:gmv AS shopify_shop_gmv_current_total,
        analytics:initial:shopify_plan_name::STRING AS initial_shopify_plan_name,
        COALESCE(wizard:builder:step = 'complete', FALSE) AS is_builder_wizard_completed,
        {{ datediff('shopify_shop_created_at_pt', 'first_installed_at_pt', 'day') }} AS age_of_store_at_install_in_days,
        {{ datediff('shopify_shop_created_at_pt', 'first_installed_at_pt', 'week') }} AS age_of_store_at_install_in_weeks,
        CASE
            WHEN age_of_store_at_install_in_days = 0 THEN '1-First Day'
            WHEN age_of_store_at_install_in_days <= 7 THEN '2-First Week (Day 2-7)'
            WHEN age_of_store_at_install_in_days <= 31 THEN '3-First Month (After First Week)'
            WHEN age_of_store_at_install_in_days <= 90 THEN '4-First Quarter (After First Month)'
            WHEN age_of_store_at_install_in_days <= 180 THEN '5-First Half (After First Quarter)'
            WHEN age_of_store_at_install_in_days <= 365 THEN '6-First Year (After First Half)'
            WHEN age_of_store_at_install_in_days <= 547 THEN '7-First 18 Months (After First Year)'
            WHEN age_of_store_at_install_in_days <= 730 THEN '8-First 2 Years (After 18 Months)'
            ELSE '9-2nd Year+'
        END AS age_of_store_at_install_bucket
    FROM {{ source_table }}
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

conversion_rates AS (
    SELECT
        currency,
        in_usd
    FROM {{ ref('currency_conversion_rates') }}
),

final AS (
    SELECT
        * EXCLUDE (shopify_shop_gmv_current_total, shopify_shop_gmv_initial_total, in_usd),
        shopify_shop_gmv_initial_total * in_usd AS shopify_shop_gmv_initial_total_usd,
        shopify_shop_gmv_current_total * in_usd AS shopify_shop_gmv_current_total_usd
    FROM decorated_shops
    LEFT JOIN activation_dates USING (shop_subdomain)
    LEFT JOIN launch_session_dates USING (shop_subdomain)
    LEFT JOIN conversion_rates USING (currency)
)

SELECT * FROM final
