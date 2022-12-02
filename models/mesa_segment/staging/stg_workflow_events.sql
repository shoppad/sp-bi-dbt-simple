{%- set source_table = source('mesa_segment', 'workflow_events') -%}
WITH
workflow_events AS (

    SELECT * FROM {{ source_table }}
    WHERE user_id NOT IN (SELECT * FROM {{ ref('staff_subdomains') }})
    AND template != 'shopify/order/send_order_report_card_email'
),

final AS (
    SELECT
        id AS workflow_event_id,
        user_id AS shop_subdomain,
        {{ groomed_column_list(source_table, except=['id', 'user_id' ]) | join(",\n       ") }}
    FROM workflow_events
)

SELECT * FROM final
