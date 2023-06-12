WITH
shops AS (
    SELECT
        shop_subdomain,
        activation_date_pt
    FROM {{ ref('int_shops') }}
)

SELECT
    *,
    IFF(workflow_run_on_pt >= activation_date_pt, 'activated', 'onboarding') AS funnel_phase
FROM {{ ref('int_workflow_runs') }}
LEFT JOIN shops USING (shop_subdomain)
