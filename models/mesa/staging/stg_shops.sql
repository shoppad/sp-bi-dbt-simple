WITH
staff_subdomains AS (
    SELECT shop_subdomain::string AS uuid
    FROM {{ ref('staff_subdomains') }}
),

grouped_shops AS (
    SELECT
        uuid::string AS shop_subdomain,
        {{ pacific_timestamp('MIN(_created_at)') }} AS first_installed_at,
        {{ pacific_timestamp('MAX(_created_at)') }} AS latest_installed_at
    FROM {{ source('mesa_mongo', 'mesa_shop_accounts') }}
    WHERE uuid NOT IN (SELECT * FROM staff_subdomains)
    GROUP BY 1
),

decorated_shops AS (
{% set columns_to_skip = ['_id', 'group', 'uuid', 'shopify', 'usage', 'config', 'webhooks', 'messages', 'analytics', '_created_at', 'schema', 'account', 'wizard', 'mongoid', 'authtoken', 'metabase'] %}
    SELECT
        uuid AS shop_subdomain,
        shopify:plan_name::string AS shopify_plan_name,
        shopify:currency::string AS currency,
        status AS install_status,
        analytics:initial:orders_count AS orders_initial_count,
        analytics:initial:orders_gmv AS revenue_initial_total,
        analytics:orders:count AS orders_count,
        analytics:orders:gmv AS revenue_total,

        {{ groomed_column_list(source('mesa_mongo','mesa_shop_accounts'), except=columns_to_skip)  | join(",\n      ") }}
    FROM {{ source('mesa_mongo','mesa_shop_accounts') }}
    WHERE NOT(__hevo__marked_deleted)
        {# TODO: I'd consider including "affiliate" #}
        AND shopify_plan_name NOT IN ('affiliate', 'partner_test', 'plus_partner_sandbox')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY _created_at DESC) = 1
),

final AS (
    SELECT *
    FROM grouped_shops
    INNER JOIN decorated_shops USING (shop_subdomain)
)

SELECT * FROM final
