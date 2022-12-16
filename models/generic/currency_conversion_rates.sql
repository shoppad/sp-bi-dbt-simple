SELECT
    SPLIT_PART("Currency", '/', 1) AS currency,
    "Value" AS in_usd,
    "Date" AS exchange_rate_at
FROM {{ source('economy_data', 'currency_conversion_rates') }}
WHERE "Indicator Name" = 'Close'
    AND "Frequency" = 'D'
    AND "Currency Unit" = 'USD'
    AND "Currency Exchange" = 'Real-time FX'
QUALIFY ROW_NUMBER() OVER (PARTITION BY "Currency Name" ORDER BY "Date" DESC) = 1
UNION ALL
SELECT
    'USD',
    1,
    CURRENT_DATE() AS exchange_rate_at
