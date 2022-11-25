{% set source_table = ref('stg_shops') %}
WITH

decorated_shops AS (
{% set columns_to_skip = ['_id', 'group', 'scopes', 'billing', 'status', 'entitlements', 'uuid', 'shopify', 'usage', 'config', 'themes', 'webhooks', 'messages', 'analytics', '_created_at', 'schema', 'account', 'wizard', 'mongoid', 'authtoken', 'metabase'] %}
    SELECT
        {{ groomed_column_list(source_table, except=columns_to_skip)  | join(",\n       ") }},
        shopify:plan_name::string AS shopify_plan_name,
        shopify:currency::string AS currency,
        status AS install_status,
        analytics:initial:orders_count AS orders_initial_count,
        analytics:initial:orders_gmv AS revenue_initial_total,
        analytics:orders:count AS orders_current_count,
        analytics:orders:gmv AS revenue_current_total
    FROM {{ source_table }}
),

final AS (
    SELECT *
    FROM decorated_shops
)

SELECT * FROM final
