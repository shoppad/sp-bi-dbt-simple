WITH shops AS (
    SELECT shop_subdomain
    FROM {{ ref('stg_shops') }}
)

SELECT
    *
FROM {{ ref('stg_workflow_steps') }}
INNER JOIN shops USING (shop_subdomain)
