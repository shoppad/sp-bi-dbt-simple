SELECT
    uuid AS shop_subdomain,
    TRIM(LOWER(e.value)) as email
FROM {{ source('php_segment', 'users') }},
LATERAL SPLIT_TO_TABLE(CONTACTS_ALLEMAILS, ',') AS e
WHERE e.value IS NOT NULL
