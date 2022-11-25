WITH shops AS (
    SELECT * FROM {{ ref('int_shops') }}
),

charges AS (
    SELECT
        *,
        _id AS charge_id,
        uuid AS shop_subdomain
    FROM {{ source('mesa_mongo', 'mesa_charges') }}
),

final AS (
    SELECT
        charge_id,
        shop_subdomain,
        subscription_id,
        billed_count,
        billed_amount,
        {{ pacific_timestamp('CREATED_AT') }} AS charged_at_pt,
        DATE_TRUNC('day', charged_at_pt)::date AS charged_on_pt
    FROM charges
    INNER JOIN shops USING (shop_subdomain)
)

SELECT * FROM final
