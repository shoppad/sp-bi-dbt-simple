{# {% set integration_apps = dbt_utils.get_column_values(table=ref('stg_workflow_steps'), column='integration_app') %} #}

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
        integration_app,
        COUNT(DISTINCT workflow_id) AS workflow_count,
        COUNT(DISTINCT workflow_id) > 1 AS is_multi_workflow,
        COUNT(workflow_steps.*) AS workflow_step_count,
        COUNT(workflow_steps.*) > 1 AS is_multi_workflow_step
    FROM int_shops
    INNER JOIN workflow_steps USING (shop_subdomain)
    {# QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain, integration_app ORDER BY shop_subdomain) = 1 #}
    GROUP BY 1, 2
)

SELECT
    *,
     integration_app IN ('{{ var("pro_apps") | join("', '") }}') AS is_pro_app
FROM final
