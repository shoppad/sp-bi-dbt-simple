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

raw_email_clicks AS (
    SELECT *
    FROM {{ source('customer_io', 'email_clicks') }}
),

raw_email_clicks_deprecated AS (
    SELECT *
    FROM {{ source('customer_io_deprecated', 'email_clicks_deprecated') }}
),

decorated_email_clicks AS (
    SELECT
        action_id,
        campaign_id,
        customer_id AS shop_subdomain,
        delivery_id,
        href,
        journey_id,
        link_id,
        recipient,
        subject,
        content_id,
        newsletter_id,
        parent_action_id,
        trigger_event_id,
        to_timestamp(__hevo__ingested_at::STRING) AS clicked_at_utc,
        {{ pacific_timestamp('clicked_at_utc') }} AS clicked_at_pt,
        IFF(journey_id IS NULL, 'broadcast', 'journey') AS email_type,
        IFF(journey_id IS NOT NULL, 'campaign-' || campaign_id || '-action-' || action_id, 'broadcast-' || newsletter_id) AS email_id
    FROM raw_email_clicks
),

decorated_email_clicks_deprecated AS (
    SELECT
        action_id,
        campaign_id,
        customer_id AS shop_subdomain,
        delivery_id,
        href,
        journey_id,
        link_id,
        recipient,
        subject,
        content_id,
        newsletter_id,
        parent_action_id,
        NULL AS trigger_event_id,
        to_timestamp(sent_at::STRING) AS clicked_at_utc,
        {{ pacific_timestamp('clicked_at_utc') }} AS clicked_at_pt,
        IFF(journey_id IS NULL, 'broadcast', 'journey') AS email_type,
        IFF(journey_id IS NOT NULL, 'campaign-' || campaign_id || '-action-' || action_id, 'broadcast-' || newsletter_id) AS email_id
    FROM raw_email_clicks_deprecated
),

combined_email_clicks AS (

    SELECT
        *
    FROM decorated_email_clicks
    UNION ALL
    SELECT
        *
    FROM decorated_email_clicks_deprecated
)

SELECT *
FROM combined_email_clicks
LEFT JOIN campaigns USING (campaign_id)
LEFT JOIN newsletters USING (newsletter_id)
WHERE campaign_id IN (SELECT campaign_id FROM campaigns)
    OR newsletter_id IN (SELECT newsletter_id FROM newsletters)
