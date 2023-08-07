WITH
campaigns AS (
    SELECT
        campaign_id,
        name AS campaign_name,
        type AS campaign_type
    FROM {{ ref('stg_campaigns') }}
),

newsletters AS (
    SELECT
        newsletter_id,
        name AS newsletter_name,
        type AS newsletter_type
    FROM {{ ref('stg_newsletters') }}
),

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
LEFT JOIN campaigns USING (campaign_id)
LEFT JOIN newsletters USING (newsletter_id)
WHERE campaign_id IN (SELECT campaign_id FROM campaigns)
    OR newsletter_id IN (SELECT newsletter_id FROM newsletters)
