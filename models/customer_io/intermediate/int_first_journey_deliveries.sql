WITH
first_journey_sends AS (
    SELECT
        recipient AS email,
        MIN(sent_at_pt) AS first_journey_sent_at_pt,
        first_journey_sent_at_pt::DATE AS first_journey_sent_on_pt,
        MIN_BY(campaign_name, sent_at_pt) AS first_journey_sent_name,
        MIN_BY(subject, sent_at_pt) AS first_journey_sent_subject
    FROM {{ ref('stg_email_sends') }}
    WHERE campaign_id IS NOT NULL
    GROUP BY 1
),

user_emails AS (
    SELECT *
    FROM {{ ref('stg_user_emails') }}
),

final AS (
    SELECT * EXCLUDE (email)
    FROM first_journey_sends
    LEFT JOIN user_emails USING (email)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY first_journey_sent_at_pt ASC) = 1
)

SELECT *
FROM final
