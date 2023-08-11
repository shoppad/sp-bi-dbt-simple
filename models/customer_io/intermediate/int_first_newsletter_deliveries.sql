WITH
first_newsletter_sends AS (
    SELECT
        recipient AS email,
        MIN(sent_at_pt) AS first_newsletter_sent_at_pt,
        first_newsletter_sent_at_pt::DATE AS first_newsletter_sent_on_pt
    FROM {{ ref('stg_email_sends') }}
    WHERE newsletter_id IS NOT NULL
    GROUP BY 1
),

user_emails AS (
    SELECT *
    FROM {{ ref('stg_user_emails') }}
),

final AS (
    SELECT * EXCLUDE (email)
    FROM first_newsletter_sends
    LEFT JOIN user_emails USING (email)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY first_newsletter_sent_at_pt ASC) = 1
)

SELECT *
FROM final
