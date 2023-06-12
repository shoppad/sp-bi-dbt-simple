WITH
raw_email_conversions AS (
    SELECT
        *
            EXCLUDE (
                __hevo_id,
                __hevo__ingested_at,
                __hevo__loaded_at
            )
            RENAME (
                customer_id AS shop_subdomain
            ),
        to_timestamp(__hevo__ingested_at::STRING) AS converted_at_utc,
        {{ pacific_timestamp('converted_at_utc') }} AS converted_at_pt,
        IFF(journey_id IS NULL, 'broadcast', 'journey') AS email_type,
        'campaign-' || campaign_id || '-action-' || action_id AS email_id
    FROM {{ source('customer_io', 'email_conversions') }}
)

SELECT *
FROM raw_email_conversions
