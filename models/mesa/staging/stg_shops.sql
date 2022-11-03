WITH
grouped_shop_ids AS (
    SELECT
        UUID AS shop_subdomain,
        MAX(_id) AS shop_id,
        ARRAY_AGG(_id::VARCHAR) AS all_shop_ids,
        {{ pacific_timestamp('MIN(_created_at)') }} AS first_installed_at,
        {{ pacific_timestamp('MAX(_created_at)') }} AS latest_installed_at
    FROM {{ source('mesa_mongo', 'mesa_shop_accounts') }}
    GROUP BY 1
),

decorated_shops AS (
{% set columns_to_skip = ['_id', 'group', 'uuid', 'shopify', 'usage', 'config', 'webhooks', 'messages', 'analytics', '_created_at', 'schema', 'account', 'wizard'] %}
    SELECT
        _id AS shop_id,
        shopify:"plan_name"::string AS shopify_plan_name,
        shopify:"currency"::string AS currency,
        status AS install_status,
        {{ groomed_column_list(source('mesa_mongo','mesa_shop_accounts'), except=columns_to_skip)  | join(",\n      ") }}
    FROM {{ source('mesa_mongo','mesa_shop_accounts') }}
    WHERE NOT(__hevo__marked_deleted)
        {# TODO: I'd consider including "affiliate" #}
        AND shopify_plan_name NOT IN ('affiliate', 'partner_test', 'plus_partner_sandbox')
),

final AS (
    SELECT *
    FROM decorated_shops
    INNER JOIN grouped_shop_ids USING (shop_id)
)

SELECT * FROM final
