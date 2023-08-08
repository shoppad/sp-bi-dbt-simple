WITH
raw_email_conversions AS (
    SELECT *
    FROM {{ source('customer_io', 'email_conversions') }}
),

raw_email_conversions_deprecated AS (
    SELECT *
    FROM {{ source('customer_io_deprecated', 'email_conversions_deprecated') }}
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
        action_id,
        campaign_id,
        customer_id AS shop_subdomain,
        delivery_id,
        journey_id,
        recipient,
        subject,
        content_id,
        newsletter_id,
        trigger_event_id,
        parent_action_id,
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
),

decorated_email_conversions_deprecated AS (

    SELECT
        action_id,
        campaign_id,
        customer_id AS shop_subdomain,
        delivery_id,
        journey_id,
        recipient,
        subject,
        content_id,
        newsletter_id,
        NULL AS trigger_event_id,
        NULL AS parent_action_id,
        to_timestamp(sent_at::STRING) AS converted_at_utc,
        {{ pacific_timestamp('converted_at_utc') }} AS converted_at_pt,
        IFF(journey_id IS NULL, 'broadcast', 'journey') AS email_type,
        CASE
            WHEN campaign_id IS NOT NULL
                THEN 'campaign-' || campaign_id || '-action-' || action_id
            ELSE
                'broadcast-' || newsletter_id
            END AS email_id
    FROM raw_email_conversions_deprecated
),

combined_email_conversions AS (

    SELECT
        *
    FROM decorated_email_conversions
    UNION ALL
    SELECT
        *
    FROM decorated_email_conversions_deprecated
)

SELECT
    *,
    COALESCE(campaign_workspace, newsletter_workspace) AS workspace
FROM combined_email_conversions
LEFT JOIN campaigns USING (campaign_id)
LEFT JOIN newsletters USING (newsletter_id)
WHERE (campaign_id IN (SELECT campaign_id FROM campaigns)
    OR newsletter_id IN (SELECT newsletter_id FROM newsletters))
