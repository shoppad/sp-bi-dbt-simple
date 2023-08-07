SELECT
    id AS campaign_id,
    name,
    type,
    'shoppad' AS workspace

FROM {{ source('customer_io', 'campaigns_shoppad') }}
WHERE tags ILIKE '%mesa%'

UNION ALL

SELECT
    id AS campaign_id,
    name,
    type,
    'marketing' AS workspace

FROM {{ source('customer_io', 'campaigns_marketing') }}
WHERE tags ILIKE '%mesa%'
