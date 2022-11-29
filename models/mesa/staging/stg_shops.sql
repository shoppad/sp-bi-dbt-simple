{%- set source_table = source('mesa_mongo', 'shops') -%}

WITH
staff_subdomains AS (
    SELECT shop_subdomain
    FROM {{ ref('staff_subdomains') }}
),

install_dates AS (
    SELECT
        uuid::string AS shop_subdomain,
        {{ pacific_timestamp('MIN(_created_at)') }} AS first_installed_at_pt,
        {{ pacific_timestamp('MAX(_created_at)') }} AS latest_installed_at_pt
    FROM {{ source_table }}
    WHERE NOT(__hevo__marked_deleted)
    GROUP BY 1
),

uninstall_dates AS (
    SELECT
        id AS shop_subdomain,
        apps_mesa_uninstalledat AS uninstalled_at_pt -- NOTE: This timestamp is already in PST
    FROM {{ source('php_segment', 'users') }}
),

shops AS (
    SELECT
        *,
        uuid::string AS shop_subdomain
    FROM {{ source_table }}
    WHERE NOT(shop_subdomain IN (SELECT * FROM staff_subdomains))
        AND NOT(__hevo__marked_deleted)
        AND shopify:plan_name NOT IN ('affiliate', 'partner_test', 'plus_partner_sandbox')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY _created_at DESC) = 1
),

plan_upgrade_dates AS (
    SELECT
        user_id AS shop_subdomain,
        MIN(timestamp)::date as plan_upgrade_date
    FROM {{ ref('stg_mesa_flow_events') }}
    WHERE event_id IN ('plan_upgrade', 'plan_select')
    GROUP BY 1
),

final AS (
    SELECT
        *,
        IFF(uninstalled_at_pt IS NULL, NULL, {{ datediff('first_installed_at_pt', 'uninstalled_at_pt', 'minute') }}) AS minutes_until_uninstall
    FROM shops
    LEFT JOIN install_dates USING (shop_subdomain)
    LEFT JOIN uninstall_dates USING (shop_subdomain)
    LEFT JOIN plan_upgrade_dates USING (shop_subdomain)
)

SELECT * FROM final
