WITH raw_mesa_plan_changes AS (
    SELECT
        COALESCE(CASE
            WHEN price NOT IN ('hybrid', 'usage')
                THEN REPLACE(price, ',', '')::FLOAT
        END, 0) AS price,
        {{ pacific_timestamp('timestamp') }} AS changed_at_pt,
        DATE_TRUNC('day', changed_at_pt)::DATE AS changed_on_pt,
        * EXCLUDE (price, handle, timestamp, {{ var('ugly_segment_fields') | join(', ') }})
            RENAME (user_id AS shop_subdomain, id AS mesa_plan_change_id)
    FROM {{ source('php_segment', 'mesa_plan_changes') }}
    WHERE handle = 'mesa'
),

shops AS (
    SELECT shop_subdomain
    FROM {{ ref('stg_shops') }}
),

final AS (
    SELECT
        *,
        LAG(price, 1) OVER (PARTITION BY shop_subdomain ORDER BY changed_at_pt) AS previous_price
    FROM shops
    INNER JOIN raw_mesa_plan_changes USING (shop_subdomain)

)

SELECT * FROM final
