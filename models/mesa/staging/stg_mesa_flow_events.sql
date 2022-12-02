{%- set source_table = source('mesa_segment', 'flow_events') %}
WITH
flow_events AS (
    SELECT
        id AS mesa_flow_event_id,
        user_id AS shop_subdomain,
        {{ groomed_column_list(source_table, except=['user_id', 'id']) | join('\n,        ') }}
    FROM {{ source_table }}
    WHERE shop_subdomain NOT IN (SELECT * FROM {{ ref('staff_subdomains') }})
)

SELECT *
FROM flow_events
