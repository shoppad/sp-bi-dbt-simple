WITH
shops AS (
    SELECT *
    FROM {{ ref('shops') }}
),

workflow_setup_counts AS (
    SELECT
        cohort_month,
        COUNT(*) AS cohort_size,
        SUM(total_revenue) AS total_revenue,
        COUNT_IF(has_a_workflow) AS has_a_workflow_count,
        COUNT_IF(has_enabled_a_workflow) AS has_enabled_a_workflow_count,
        COUNT_IF(is_activated) AS is_activated_count,
        has_a_workflow_count / NULLIF(cohort_size, 0) AS has_a_workflow_pct,
        has_enabled_a_workflow_count / NULLIF(has_a_workflow_count, 0) AS workflow_enabling_incremental_pct,
        has_enabled_a_workflow_count / NULLIF(cohort_size, 0) AS workflow_enabling_cohort_pct,
        is_activated_count / NULLIF(cohort_size, 0) AS activation_cohort_pct,
        is_activated_count / NULLIF(has_enabled_a_workflow_count, 0) AS activation_incremental_pct
    FROM shops
    GROUP BY 1
),

final AS (

    SELECT
        *,
        total_revenue / NULLIF(cohort_size, 0) AS lifetime_value_installed,
        total_revenue / NULLIF(has_a_workflow_count, 0) AS lifetime_value_has_a_workflow,
        total_revenue / NULLIF(has_enabled_a_workflow_count, 0) AS lifetime_value_enabled_workflow,
        total_revenue / NULLIF(is_activated_count, 0) AS lifetime_value_activated
    FROM workflow_setup_counts
)

SELECT * FROM final
