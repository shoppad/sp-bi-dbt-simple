WITH

raw_daus AS (
    SELECT
        * EXCLUDE (uuid),
        uuid AS shop_subdomain
    FROM {{ source('mongo_sync', 'legacy_DAUs') }}
    -- Possibly run a query that grabs the MIN() on the mesa_charges model for usage charges.
)

SELECT *
FROM raw_daus
