WITH

shop_days AS (

    SELECT *
    FROM {{ ref('int_mesa_shop_days') }}

),

final AS (

    SELECT *
    FROM shop_days

)

SELECT * FROM final
