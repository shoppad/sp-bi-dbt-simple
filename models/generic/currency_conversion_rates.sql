SELECT
    quote_currency_id AS currency,
    value AS in_usd,
    date AS exchange_rate_at
FROM {{ source('economy_data', 'currency_conversion_rates') }}
WHERE base_currency_id = 'USD'
{# QUALIFY ROW_NUMBER() OVER (PARTITION BY "Currency Name" ORDER BY "Date" DESC) = 1 #}
UNION ALL
SELECT
    'USD' AS currency,
    1 AS in_usd,
    CURRENT_DATE() AS exchange_rate_at
