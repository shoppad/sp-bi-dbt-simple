WITH staged_shop_infos AS (
    SELECT *
    FROM {{ ref('stg_shop_infos') }}
),

staged_constellation_users AS (
    SELECT *
    FROM {{ ref('stg_constellation_users') }}
)

SELECT *
FROM staged_shop_infos
FULL OUTER JOIN staged_constellation_users USING (shop_subdomain)
