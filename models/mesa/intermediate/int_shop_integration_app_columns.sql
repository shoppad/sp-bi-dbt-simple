{% set integration_apps = dbt_utils.get_column_values(table=ref('stg_workflow_steps'), column='integration_app') %}

WITH

int_shops AS (
    SELECT
        shop_subdomain
    FROM {{ ref('stg_shops') }}
),

enabled_workflows AS (
    SELECT
        workflow_id
    FROM {{ ref('stg_workflows') }}
    WHERE is_enabled AND NOT is_deleted
),

workflow_steps AS (
    SELECT
        shop_subdomain,
        integration_app,
        workflow_id
    FROM {{ ref('stg_workflow_steps') }}
    WHERE workflow_id IN (SELECT workflow_id FROM enabled_workflows)
),

final AS (
    SELECT
        shop_subdomain,
        {% for integration_app in integration_apps %}
            COUNT_IF(integration_app = '{{ integration_app }}') > 0 AS uses_app_{{ integration_app | replace('-', '_') }},
            COUNT_IF(integration_app = '{{ integration_app }}') AS app_{{ integration_app | replace('-', '_') }}_step_count
            {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM int_shops
    LEFT JOIN workflow_steps USING (shop_subdomain)
    GROUP BY 1
)

SELECT *
FROM final
