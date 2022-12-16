{% set source_table = ref('stg_shops') %}
WITH
decorated_shops AS (
{% set columns_to_skip = ['scopes', 'billing', 'status', 'entitlements', 'timestamp', 'shopify', 'usage', 'config', 'themes', 'webhooks', 'messages', 'analytics', 'schema', 'handle', 'method', 'account', 'wizard', 'mongoid', 'authtoken', 'metabase'] %}
    SELECT
        {{ groomed_column_list(source_table, except=columns_to_skip)  | join(",\n       ") }},
        shopify:plan_name::string AS shopify_plan_name,
        shopify:currency::string AS currency,
        status AS install_status,
        analytics:initial:orders_count AS orders_initial_count,
        analytics:initial:orders_gmv AS revenue_initial_total,
        analytics:orders:count AS orders_current_count,
        analytics:orders:gmv AS revenue_current_total,
        wizard:builder:step = 'complete' AS is_builder_wizard_completed
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
        meta_attribs.value:value::DATE AS launch_session_date
    FROM {{ ref('stg_shops') }},
        LATERAL FLATTEN(input => meta) AS meta_attribs
    WHERE meta_attribs.value:name = 'launchsessiondate'
),

boolean_launch_sessions AS (
  SELECT
        shop_subdomain,
        meta_attribs.value:value = 'enabled' AS has_had_launch_session
    FROM {{ ref('stg_shops') }},
        LATERAL FLATTEN(input => meta) AS meta_attribs
    WHERE meta_attribs.value:name = 'hadlaunchsession'
),

conversion_rates AS (
    SELECT
        currency,
        in_usd
    FROM {{ ref('currency_conversion_rates') }}
),

final AS (
    SELECT * EXCLUDE (revenue_current_total, revenue_initial_total, in_usd),
    revenue_initial_total * in_usd AS revenue_initial_total_usd,
    revenue_current_total * in_usd AS revenue_current_total_usd

    FROM decorated_shops
    LEFT JOIN activation_dates USING (shop_subdomain)
    LEFT JOIN launch_session_dates USING (shop_subdomain)
    LEFT JOIN boolean_launch_sessions USING (shop_subdomain)
    LEFT JOIN conversion_rates USING (currency)
)

SELECT * FROM final
