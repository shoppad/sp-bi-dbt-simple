WITH constellation_users AS (
    SELECT
        uuid AS shop_subdomain,
        createdat AS first_installed_at,
        analytics_gmv,
        analytics_orders,
        shopify_createdat,
        shopify_inactiveat,
        shopify_plandisplayname,
        shopify_planname,
        analytics_orders >= 50 AND shopify_createdat <= CURRENT_DATE - INTERVAL '2 years' AS is_mql

    FROM {{ source('php_segment', 'users') }}
    WHERE shop_subdomain NOT IN (SELECT * FROM {{ ref('staff_subdomains') }})
)

SELECT *
FROM constellation_users
