{%- set source_table = source('mesa_mongo', 'shops') -%}

WITH
staff_subdomains AS (
    SELECT shop_subdomain
    FROM {{ ref('staff_subdomains') }}
),

shop_dates AS (
    SELECT
        uuid::string AS shop_subdomain,
        {{ pacific_timestamp('MIN(_created_at)') }} AS first_installed_at,
        {{ pacific_timestamp('MAX(_created_at)') }} AS latest_installed_at
    FROM {{ source_table }}
    WHERE NOT(__hevo__marked_deleted)
    GROUP BY 1
),


shops AS (
    SELECT
        *,
        uuid::string AS shop_subdomain
    FROM {{ source_table }}
    WHERE NOT(shop_subdomain IN (SELECT * FROM staff_subdomains))
        AND NOT(__hevo__marked_deleted)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY _created_at DESC) = 1
)

SELECT * FROM shops
LEFT JOIN shop_dates USING (shop_subdomain)
