SELECT
    * RENAME ID AS newsletter_id,
    'shoppad' AS workspace
FROM {{ source('customer_io', 'newsletters_shoppad') }}
WHERE (tags ILIKE '%mesa%' OR tags NOT ILIKE '%app%')

UNION ALL

SELECT
    * RENAME ID AS newsletter_id,
    'marketing' AS workspace
FROM {{ source('customer_io', 'newsletters_marketing') }}
WHERE (tags ILIKE '%mesa%' OR tags NOT ILIKE '%app%')
