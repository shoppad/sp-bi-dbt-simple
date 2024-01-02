with

source AS (
    SELECT *
    FROM {{ ref("int_ga4_events") }}
    WHERE
        page_location ilike '%apps.shopify.com%'
        OR event_name ilike 'shopify%'
        OR page_location ilike '%surface_%'

)

SELECT *
FROM source
