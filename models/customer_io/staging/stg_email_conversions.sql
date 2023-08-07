WITH
raw_email_conversions AS (
    SELECT *
    FROM {{ source('customer_io', 'email_conversions') }}
),

campaigns AS (
    SELECT
        campaign_id,
        name AS campaign_name,
        type AS campaign_type,
        workspace AS campaign_workspace
    FROM {{ ref('stg_campaigns') }}
),

newsletters AS (
    SELECT
        newsletter_id,
        name AS newsletter_name,
        type AS newsletter_type,
        workspace AS newsletter_workspace
    FROM {{ ref('stg_newsletters') }}
),

decorated_email_conversions AS (
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
        CASE
            WHEN campaign_id IS NOT NULL
                THEN 'campaign-' || campaign_id || '-action-' || action_id
            ELSE
                'broadcast-' || newsletter_id
            END AS email_id
    FROM raw_email_conversions
)

SELECT
    *,
    COALESCE(campaign_workspace, newsletter_workspace) AS workspace
FROM decorated_email_conversions
LEFT JOIN campaigns USING (campaign_id)
LEFT JOIN newsletters USING (newsletter_id)
WHERE (campaign_id IN (SELECT campaign_id FROM campaigns)
    OR newsletter_id IN (SELECT newsletter_id FROM newsletters))
