WITH staged_shop_infos AS (
    SELECT *
    FROM {{ ref('stg_shop_infos') }}
),

staged_constellation_users AS (
    SELECT *
    FROM {{ ref('stg_constellation_users') }}
),

final AS (
    SELECT
        staged_shop_infos.* EXCLUDE (updated_at, shop_subdomain),
        staged_constellation_users.* EXCLUDE (updated_at, shop_subdomain),
        COALESCE(staged_shop_infos.shop_subdomain, staged_constellation_users.shop_subdomain) AS shop_subdomain,
        COALESCE(staged_shop_infos.updated_at, staged_constellation_users.updated_at) AS updated_at
    FROM staged_shop_infos
    FULL OUTER JOIN staged_constellation_users USING (shop_subdomain)
)

SELECT *
FROM final
