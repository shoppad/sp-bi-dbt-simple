WITH
raw_email_opens AS (
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
        to_timestamp(__hevo__ingested_at::STRING) AS opened_at_utc,
        {{ pacific_timestamp('opened_at_utc') }} AS opened_at_pt,
        IFF(journey_id IS NULL, 'broadcast', 'journey') AS email_type,
        IFF(journey_id IS NOT NULL, 'campaign-' || campaign_id || '-action-' || action_id, 'broadcast-' || newsletter_id) AS email_id
    FROM {{ source('customer_io', 'email_opens') }}
)

SELECT *
FROM raw_email_opens
