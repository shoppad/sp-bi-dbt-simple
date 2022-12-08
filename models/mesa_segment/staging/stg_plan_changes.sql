WITH raw_plan_changes AS (
    SELECT
        id AS plan_change_id,
        user_id AS shop_subdomain,
        CASE
            WHEN price NOT IN ('hybrid', 'usage')
                THEN REPLACE(price, ',', '')::NUMERIC
        END AS price,
        {{ pacific_timestamp('timestamp') }} AS changed_at_pt,
        date_trunc('day', changed_at_pt)::DATE AS changed_on_pt,
        {{ var('ugly_segment_fields') | join(',\n       ') }},
        * EXCLUDE (user_id, id, price, timestamp, {{ var('ugly_segment_fields') | join(', ') }})
    FROM {{ source('php_segment', 'plan_changes') }}
    WHERE handle = 'mesa'
),

shops AS (
    SELECT shop_subdomain
    FROM {{ ref('stg_shops') }}
),

final AS (
    SELECT *
    FROM shops
    LEFT JOIN raw_plan_changes USING (shop_subdomain)

)

SELECT * FROM final
