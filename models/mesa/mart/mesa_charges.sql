WITH shops AS (
    SELECT * FROM {{ ref('stg_shops') }}
),

charges AS (
    SELECT
        *,
        _id AS charge_id,
        merchant_id AS shop_id
    FROM {{ source('mesa_mongo', 'mesa_charges') }}
),

final AS (
    SELECT
        shops.shop_id,
        shop_subdomain,
        subscription_id,
        billed_count,
        billed_amount,
        {{ pacific_timestamp('CREATED_AT') }} AS charged_at_pt,
        DATE_TRUNC('day', charged_at_pt)::date AS charged_on_pt
    FROM charges
    INNER JOIN shops ON (array_contains(charges.shop_id::variant, shops.all_shop_ids))
)

SELECT * FROM final
