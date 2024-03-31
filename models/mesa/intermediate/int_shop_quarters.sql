WITH
shop_months AS (
    SELECT *
    FROM {{ ref('int_shop_months') }}
),

qau AS (
    SELECT
        shop_subdomain,
        date_trunc('quarter', month) AS quarter,
        sum(inc_amount) AS inc_amount
    FROM shop_months
    GROUP BY
        1,
        2
),

first_qt AS (
    SELECT
        shop_subdomain,
        min(quarter) AS first_quarter
    FROM qau
    GROUP BY 1
),

qau_decorated AS (
    SELECT
        qau.quarter,
        qau.shop_subdomain,
        qau.inc_amount,
        first_qt.first_quarter
    FROM qau
    INNER JOIN first_qt ON (qau.shop_subdomain = first_qt.shop_subdomain) and qau.inc_amount > 0
)

SELECT * FROM qau_decorated ORDER BY first_quarter, quarter, shop_subdomain
