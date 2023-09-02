WITH
raw_shops AS (
    SELECT
        * RENAME uuid AS shop_subdomain
    FROM {{ source('mongo_sync', 'shops') }}
    WHERE
        NOT __hevo__marked_deleted
        AND shopify:plan_name NOT IN ('staff', 'staff_business', 'shopify_alumni')

),

trimmed_shops AS (
    {% set exclude = ['_id', '_created_at', 'timestamp', 'method'] + var('etl_fields') -%}

    SELECT
        * EXCLUDE ({{ exclude | join(', ') }}),
        _created_at AS created_at
    FROM raw_shops
),

staff_subdomains AS (
    SELECT shop_subdomain
    FROM {{ ref('staff_subdomains') }}
),

custom_apps AS (
    SELECT
        shop_subdomain,
        TRUE AS is_custom_app,
        'Custom App' AS status,
        PARSE_JSON('{"plan_name": "None (Custom App)", "currency": "USD"}') AS shopify,
        first_dt,
        last_dt
    FROM {{ ref('custom_app_daily_revenues') }}
),

install_dates AS (
    SELECT
        shop_subdomain,
        MIN(COALESCE(created_at, first_dt)) AS first_installed_at_utc,
        MAX(COALESCE(created_at, first_dt)) AS latest_installed_at_utc,
        {{ pacific_timestamp('MIN(COALESCE(created_at, first_dt))') }} AS first_installed_at_pt,
        {{ pacific_timestamp('MIN(COALESCE(created_at, first_dt))') }}::DATE AS first_installed_on_pt,
        {{ pacific_timestamp('MAX(COALESCE(created_at, first_dt))') }} AS latest_installed_at_pt,
        DATE_TRUNC('week', first_installed_at_pt)::DATE AS cohort_week,
        DATE_TRUNC('month', first_installed_at_pt)::DATE AS cohort_month
    FROM trimmed_shops
    FULL OUTER JOIN custom_apps USING (shop_subdomain)
    GROUP BY 1
),


shop_metas AS (
    SELECT
        shop_subdomain,
        ARRAY_UNION_AGG(meta) AS aggregated_meta
    FROM trimmed_shops
    GROUP BY 1
),

shops AS (
    SELECT * EXCLUDE ("META")
    FROM trimmed_shops
    WHERE
        NOT shop_subdomain IN (SELECT * FROM staff_subdomains)
        AND shopify:plan_name NOT IN ('affiliate', 'partner_test', 'plus_partner_sandbox')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY created_at DESC) = 1
),

uninstall_data_points AS (
    SELECT
        shop_subdomain,
        IFF(shops.status = 'active', NULL, uninstalled_at_pt) AS uninstalled_at_pt
    FROM shops
    LEFT JOIN (
        SELECT
            id AS shop_subdomain,
            apps_mesa_uninstalledat AS uninstalled_at_pt-- NOTE: This timestamp is already in PST
        FROM {{ source('php_segment', 'users') }}

        UNION ALL
        SELECT
            shop_subdomain,
            uninstalled_at_pt -- NOTE: This timestamp is already in PST
        FROM {{ ref('stg_mesa_uninstalls') }}

        UNION ALL
        SELECT
            shop_subdomain,
            last_dt AS uninstalled_at_pt
        FROM custom_apps
    ) USING (shop_subdomain)
),

uninstall_dates AS (
    SELECT
        shop_subdomain,
        MAX(uninstalled_at_pt) AS uninstalled_at_pt
    FROM uninstall_data_points
    GROUP BY 1
),

final AS (
    SELECT
        * EXCLUDE (created_at, "GROUP", aggregated_meta, is_custom_app, first_dt, last_dt, shopify, status),
        COALESCE(shops.status, custom_apps.status) AS status,
        shop_metas.aggregated_meta AS meta,
        COALESCE(shops.shopify, custom_apps.shopify) AS shopify,
        TO_TIMESTAMP_NTZ(billing:plan:trial_ends::VARCHAR)::DATE AS trial_end_dt,
        IFF(uninstalled_at_pt IS NULL, NULL, {{ datediff('first_installed_at_pt', 'uninstalled_at_pt', 'minute') }}) AS minutes_until_uninstall,
        COALESCE(is_custom_app, FALSE) AS is_custom_app
    FROM shops
    FULL OUTER JOIN custom_apps USING (shop_subdomain)
    LEFT JOIN shop_metas USING (shop_subdomain)
    LEFT JOIN install_dates USING (shop_subdomain)
    LEFT JOIN uninstall_dates USING (shop_subdomain)
)

SELECT * FROM final
