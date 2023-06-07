WITH
raw_shopify_plan_changes AS (
    SELECT
        * EXCLUDE (timestamp, handle, {{ var('ugly_segment_fields') | join(', ') }})
            RENAME (id AS shopify_plan_change_id, user_id AS shop_subdomain),
        {{ pacific_timestamp('timestamp') }} AS changed_at_pt,
        cast(changed_at_pt AS DATE) AS changed_on_pt
    FROM {{ source('php_segment', 'shopify_plan_changes') }}
),

shops AS (
    SELECT shop_subdomain
    FROM {{ ref('stg_shops') }}
)

SELECT *
FROM shops
INNER JOIN raw_shopify_plan_changes USING (shop_subdomain)
