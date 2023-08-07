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
)

SELECT
    * RENAME customer_id AS shop_subdomain,
    'shoppad' AS workspace,
    IFF(newsletter_id IS NOT NULL, 'broadcast', 'journey') AS email_unsubscribe_email_type,
    CASE
        WHEN (newsletter_id IS NOT NULL AND newsletter_name IS NULL)
            OR (campaign_id IS NOT NULL AND campaign_name IS NULL)
            THEN '*[Unsubscribed via Non-MESA Email]*'
        WHEN newsletter_id IS NOT NULL
            THEN newsletter_name
        ELSE
            campaign_name
        END AS email_unsubscribe_email_name

FROM {{ source('customer_io', 'email_unsubscribes_shoppad') }}
LEFT JOIN campaigns USING (campaign_id)
LEFT JOIN newsletters USING (newsletter_id)
