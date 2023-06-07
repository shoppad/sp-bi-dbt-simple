WITH
raw_email_opens AS (
    SELECT
        *
            EXCLUDE (
                context_integration_name,
                context_integration_version,
                user_id,
                timestamp,
                {{ var('ugly_segment_fields') | join(', ') }}
            )
            RENAME customer_id AS shop_subdomain,
        {{ pacific_timestamp('original_timestamp') }} AS opened_at_pt,
        IFF(action_id IS NOT NULL, 'action-' || action_id, 'newsletter-' || newsletter_id) AS email_id
    FROM {{ source('customer_io', 'email_opens') }}
)

SELECT *
FROM raw_email_opens
