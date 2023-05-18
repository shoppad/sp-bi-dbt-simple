WITH raw_mesa_plan_changes AS (
    SELECT
        id AS mesa_plan_change_id,
        user_id AS shop_subdomain,
        COALESCE(CASE
            WHEN price NOT IN ('hybrid', 'usage')
                THEN REPLACE(price, ',', '')::NUMERIC
        END, 0) AS price,
        {{ pacific_timestamp('timestamp') }} AS changed_at_pt,
        DATE_TRUNC('day', changed_at_pt)::DATE AS changed_on_pt,
        * EXCLUDE (user_id, id, price, timestamp, {{ var('ugly_segment_fields') | join(', ') }})
    FROM {{ source('php_segment', 'mesa_plan_changes') }}
    WHERE handle = 'mesa'
),

shops AS (
    SELECT shop_subdomain
    FROM {{ ref('stg_shops') }}
),

final AS (
    SELECT *
    FROM shops
    INNER JOIN raw_mesa_plan_changes USING (shop_subdomain)

)

SELECT * FROM final
