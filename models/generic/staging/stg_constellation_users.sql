WITH constellation_users AS (
    SELECT
        COALESCE(uuid, id) AS shop_subdomain,
        createdat AS first_in_constellation_at_utc,
        {{ pacific_timestamp('first_in_constellation_at_utc') }} AS first_in_constellation_at_pt,
        first_in_constellation_at_pt::DATE AS first_in_constellation_on_pt,
        date_trunc('week', first_in_constellation_on_pt) AS constellation_cohort_week,
        COALESCE(updatedat, createdat, uuid_ts) AS updated_at,
        analytics_gmv,
        analytics_orders,
        shopify_createdat,
        shopify_inactiveat,
        shopify_plandisplayname,
        shopify_planname,
        analytics_orders >= 50 AND shopify_createdat <= CURRENT_DATE - INTERVAL '2 years' AS is_mql

    FROM {{ source('php_segment', 'users') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY createdat DESC) = 1
)

SELECT *
FROM constellation_users
