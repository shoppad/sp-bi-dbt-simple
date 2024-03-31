WITH
latest_current_rates_in_usd AS (
    SELECT
        quote_currency_id AS currency,
        MAX_BY(value, date) AS in_usd,
        MAX(date) AS exchange_rate_at
    FROM {{ source('economy_data', 'currency_conversion_rates') }}
    WHERE base_currency_id = 'USD'
    GROUP BY 1
)

SELECT *
FROM latest_current_rates_in_usd

UNION ALL

SELECT
    'USD' AS currency,
    1 AS in_usd,
    CURRENT_DATE() AS exchange_rate_at
