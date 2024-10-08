WITH
shops AS (
    SELECT *
    FROM {{ ref('shops') }}
    WHERE is_mql
),

workflow_step_counts AS (
    SELECT
        cohort_month,
        COUNT(*) AS cohort_size,
        SUM(total_ltv_revenue) AS total_ltv_revenue,
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

workflows AS (
    SELECT *
    FROM {{ ref('workflows') }}
),

workflows_with_time_ranges AS (
    SELECT
        shop_subdomain,
        COUNT_IF(workflows.created_at_pt < DATEADD('day', 1, first_installed_at_pt)) AS _workflow_first_day_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('day', 2, first_installed_at_pt)) AS _workflow_first_two_days_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('day', 3, first_installed_at_pt)) AS _workflow_first_three_days_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('week', 1, first_installed_at_pt)) AS _workflow_first_week_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('week', 2, first_installed_at_pt)) AS _workflow_first_two_weeks_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('month', 1, first_installed_at_pt)) AS _workflow_first_month_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('month', 2, first_installed_at_pt)) AS _workflow_first_two_months_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('month', 3, first_installed_at_pt)) AS _workflow_first_three_months_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('month', 6, first_installed_at_pt)) AS _workflow_first_six_months_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('year', 1, first_installed_at_pt)) AS _workflow_first_year_count
    FROM shops
    LEFT JOIN workflows USING (shop_subdomain)
    GROUP BY 1
),

workflows_created_time_buckets AS (
    SELECT
        cohort_month,
        COUNT_IF(_workflow_first_day_count > 0) AS has_workflow_first_day_count,
        has_workflow_first_day_count / COUNT(*) AS has_workflow_first_day_pct,
        COUNT_IF(_workflow_first_two_days_count > 0) AS has_workflow_first_two_days_count,
        has_workflow_first_two_days_count / COUNT(*) AS has_workflow_first_two_days_pct,
        COUNT_IF(_workflow_first_three_days_count > 0) AS has_workflow_first_three_days_count,
        has_workflow_first_three_days_count / COUNT(*) AS has_workflow_first_three_days_pct,
        COUNT_IF(_workflow_first_week_count > 0) AS has_workflow_first_week_count,
        has_workflow_first_week_count / COUNT(*) AS has_workflow_first_week_pct,
        COUNT_IF(_workflow_first_two_weeks_count > 0) AS has_workflow_first_two_weeks_count,
        has_workflow_first_two_weeks_count / COUNT(*) AS has_workflow_first_two_weeks_pct,
        COUNT_IF(_workflow_first_month_count > 0) AS has_workflow_first_month_count,
        has_workflow_first_month_count / COUNT(*) AS has_workflow_first_month_pct,
        COUNT_IF(_workflow_first_two_months_count > 0) AS has_workflow_first_two_months_count,
        has_workflow_first_two_months_count / COUNT(*) AS has_workflow_first_two_months_pct,
        COUNT_IF(_workflow_first_three_months_count > 0) AS has_workflow_first_three_months_count,
        has_workflow_first_three_months_count / COUNT(*) AS has_workflow_first_three_months_pct,
        COUNT_IF(_workflow_first_six_months_count > 0) AS has_workflow_first_six_months_count,
        has_workflow_first_six_months_count / COUNT(*) AS has_workflow_first_six_months_pct,
        COUNT_IF(_workflow_first_year_count > 0) AS has_workflow_first_year_count,
        has_workflow_first_year_count / COUNT(*) AS has_workflow_first_year_pct
    FROM shops
    LEFT JOIN workflows_with_time_ranges USING (shop_subdomain)
    GROUP BY 1
),

enabled_funnel_achievements AS (
    SELECT
        shop_subdomain,
        achieved_at_pt
    FROM {{ ref('int_mesa_shop_funnel_achievements') }}
    WHERE step_order >= 6
),

workflows_enabled_time_buckets AS (
    SELECT
        cohort_month,
        COUNT_IF(achieved_at_pt < DATEADD('day', 1, first_installed_at_pt)) AS has_workflow_enabled_first_day_count,
        COUNT_IF(achieved_at_pt < DATEADD('day', 2, first_installed_at_pt)) AS has_workflow_enabled_first_two_days_count,
        COUNT_IF(achieved_at_pt < DATEADD('day', 3, first_installed_at_pt)) AS has_workflow_enabled_first_three_days_count,
        COUNT_IF(achieved_at_pt < DATEADD('week', 1, first_installed_at_pt)) AS has_workflow_enabled_first_week_count,
        COUNT_IF(achieved_at_pt < DATEADD('week', 2, first_installed_at_pt)) AS has_workflow_enabled_first_two_weeks_count,
        COUNT_IF(achieved_at_pt < DATEADD('month', 1, first_installed_at_pt)) AS has_workflow_enabled_first_month_count,
        COUNT_IF(achieved_at_pt < DATEADD('month', 2, first_installed_at_pt)) AS has_workflow_enabled_first_two_months_count,
        COUNT_IF(achieved_at_pt < DATEADD('month', 3, first_installed_at_pt)) AS has_workflow_enabled_first_three_months_count,
        COUNT_IF(achieved_at_pt < DATEADD('month', 6, first_installed_at_pt)) AS has_workflow_enabled_first_six_months_count,
        COUNT_IF(achieved_at_pt < DATEADD('year', 1, first_installed_at_pt)) AS has_workflow_enabled_first_year_count,

        has_workflow_enabled_first_day_count / COUNT(*) AS has_workflow_enabled_first_day_pct,
        has_workflow_enabled_first_two_days_count / COUNT(*) AS has_workflow_enabled_first_two_days_pct,
        has_workflow_enabled_first_three_days_count / COUNT(*) AS has_workflow_enabled_first_three_days_pct,
        has_workflow_enabled_first_week_count / COUNT(*) AS has_workflow_enabled_first_week_pct,
        has_workflow_enabled_first_two_weeks_count / COUNT(*) AS has_workflow_enabled_first_two_weeks_pct,
        has_workflow_enabled_first_month_count / COUNT(*) AS has_workflow_enabled_first_month_pct,
        has_workflow_enabled_first_two_months_count / COUNT(*) AS has_workflow_enabled_first_two_months_pct,
        has_workflow_enabled_first_three_months_count / COUNT(*) AS has_workflow_enabled_first_three_months_pct,
        has_workflow_enabled_first_six_months_count / COUNT(*) AS has_workflow_enabled_first_six_months_pct,
        has_workflow_enabled_first_year_count / COUNT(*) AS has_workflow_enabled_first_year_pct
    FROM shops
    LEFT JOIN enabled_funnel_achievements USING (shop_subdomain)
    GROUP BY 1
),

plan_upgrade_counts AS (
    SELECT
        cohort_month,
        COUNT_IF(has_done_a_trial) AS has_done_a_trial_count,
        has_done_a_trial_count / COUNT(*) AS ever_did_trial_pct,
        COUNT_IF(has_ever_upgraded_to_paid_plan) AS ever_paid_plan_count,
        ever_paid_plan_count / COUNT(*) AS ever_paid_plan_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('day', 1, first_installed_at_pt)) AS paid_plan_first_day_count,
        paid_plan_first_day_count / COUNT(*) AS paid_plan_first_day_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('day', 2, first_installed_at_pt)) AS paid_plan_first_two_days_count,
        paid_plan_first_two_days_count / COUNT(*) AS paid_plan_first_two_days_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('day', 3, first_installed_at_pt)) AS paid_plan_first_three_days_count,
        paid_plan_first_three_days_count / COUNT(*) AS paid_plan_first_three_days_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('week', 1, first_installed_at_pt)) AS paid_plan_first_week_count,
        paid_plan_first_week_count / COUNT(*) AS paid_plan_first_week_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('week', 2, first_installed_at_pt)) AS paid_plan_first_two_weeks_count,
        paid_plan_first_two_weeks_count / COUNT(*) AS paid_plan_first_two_weeks_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 1, first_installed_at_pt)) AS paid_plan_first_month_count,
        paid_plan_first_month_count / COUNT(*) AS paid_plan_first_month_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 2, first_installed_at_pt)) AS paid_plan_first_two_months_count,
        paid_plan_first_two_months_count / COUNT(*) AS paid_plan_first_two_months_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 3, first_installed_at_pt)) AS paid_plan_first_three_months_count,
        paid_plan_first_three_months_count / COUNT(*) AS paid_plan_first_three_months_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 6, first_installed_at_pt)) AS paid_plan_first_six_months_count,
        paid_plan_first_six_months_count / COUNT(*) AS paid_plan_first_six_months_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('year', 1, first_installed_at_pt)) AS paid_plan_first_year_count,
        paid_plan_first_year_count / COUNT(*) AS paid_plan_first_year_pct
    FROM shops
    GROUP BY 1
),

workflows AS (
    SELECT *
    FROM {{ ref('workflows') }}
),

workflows_with_time_ranges AS (
    SELECT
        shop_subdomain,
        COUNT_IF(workflows.created_at_pt < DATEADD('day', 1, first_installed_at_pt)) AS _workflow_first_day_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('day', 2, first_installed_at_pt)) AS _workflow_first_two_days_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('day', 3, first_installed_at_pt)) AS _workflow_first_three_days_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('week', 1, first_installed_at_pt)) AS _workflow_first_week_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('week', 2, first_installed_at_pt)) AS _workflow_first_two_weeks_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('month', 1, first_installed_at_pt)) AS _workflow_first_month_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('month', 2, first_installed_at_pt)) AS _workflow_first_two_months_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('month', 3, first_installed_at_pt)) AS _workflow_first_three_months_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('month', 6, first_installed_at_pt)) AS _workflow_first_six_months_count,
        COUNT_IF(workflows.created_at_pt < DATEADD('year', 1, first_installed_at_pt)) AS _workflow_first_year_count
    FROM shops
    LEFT JOIN workflows USING (shop_subdomain)
    GROUP BY 1
),

workflows_created_time_buckets AS (
    SELECT
        cohort_month,
        COUNT_IF(_workflow_first_day_count > 0) AS has_workflow_first_day_count,
        has_workflow_first_day_count / COUNT(*) AS has_workflow_first_day_pct,
        COUNT_IF(_workflow_first_two_days_count > 0) AS has_workflow_first_two_days_count,
        has_workflow_first_two_days_count / COUNT(*) AS has_workflow_first_two_days_pct,
        COUNT_IF(_workflow_first_three_days_count > 0) AS has_workflow_first_three_days_count,
        has_workflow_first_three_days_count / COUNT(*) AS has_workflow_first_three_days_pct,
        COUNT_IF(_workflow_first_week_count > 0) AS has_workflow_first_week_count,
        has_workflow_first_week_count / COUNT(*) AS has_workflow_first_week_pct,
        COUNT_IF(_workflow_first_two_weeks_count > 0) AS has_workflow_first_two_weeks_count,
        has_workflow_first_two_weeks_count / COUNT(*) AS has_workflow_first_two_weeks_pct,
        COUNT_IF(_workflow_first_month_count > 0) AS has_workflow_first_month_count,
        has_workflow_first_month_count / COUNT(*) AS has_workflow_first_month_pct,
        COUNT_IF(_workflow_first_two_months_count > 0) AS has_workflow_first_two_months_count,
        has_workflow_first_two_months_count / COUNT(*) AS has_workflow_first_two_months_pct,
        COUNT_IF(_workflow_first_three_months_count > 0) AS has_workflow_first_three_months_count,
        has_workflow_first_three_months_count / COUNT(*) AS has_workflow_first_three_months_pct,
        COUNT_IF(_workflow_first_six_months_count > 0) AS has_workflow_first_six_months_count,
        has_workflow_first_six_months_count / COUNT(*) AS has_workflow_first_six_months_pct,
        COUNT_IF(_workflow_first_year_count > 0) AS has_workflow_first_year_count,
        has_workflow_first_year_count / COUNT(*) AS has_workflow_first_year_pct
    FROM shops
    LEFT JOIN workflows_with_time_ranges USING (shop_subdomain)
    GROUP BY 1
),

enabled_funnel_achievements AS (
    SELECT
        shop_subdomain,
        achieved_at_pt
    FROM {{ ref('int_mesa_shop_funnel_achievements') }}
    WHERE step_order >= 6
),

workflows_enabled_time_buckets AS (
    SELECT
        cohort_month,
        COUNT_IF(achieved_at_pt < DATEADD('day', 1, first_installed_at_pt)) AS has_workflow_enabled_first_day_count,
        COUNT_IF(achieved_at_pt < DATEADD('day', 2, first_installed_at_pt)) AS has_workflow_enabled_first_two_days_count,
        COUNT_IF(achieved_at_pt < DATEADD('day', 3, first_installed_at_pt)) AS has_workflow_enabled_first_three_days_count,
        COUNT_IF(achieved_at_pt < DATEADD('week', 1, first_installed_at_pt)) AS has_workflow_enabled_first_week_count,
        COUNT_IF(achieved_at_pt < DATEADD('week', 2, first_installed_at_pt)) AS has_workflow_enabled_first_two_weeks_count,
        COUNT_IF(achieved_at_pt < DATEADD('month', 1, first_installed_at_pt)) AS has_workflow_enabled_first_month_count,
        COUNT_IF(achieved_at_pt < DATEADD('month', 2, first_installed_at_pt)) AS has_workflow_enabled_first_two_months_count,
        COUNT_IF(achieved_at_pt < DATEADD('month', 3, first_installed_at_pt)) AS has_workflow_enabled_first_three_months_count,
        COUNT_IF(achieved_at_pt < DATEADD('month', 6, first_installed_at_pt)) AS has_workflow_enabled_first_six_months_count,
        COUNT_IF(achieved_at_pt < DATEADD('year', 1, first_installed_at_pt)) AS has_workflow_enabled_first_year_count,

        has_workflow_enabled_first_day_count / COUNT(*) AS has_workflow_enabled_first_day_pct,
        has_workflow_enabled_first_two_days_count / COUNT(*) AS has_workflow_enabled_first_two_days_pct,
        has_workflow_enabled_first_three_days_count / COUNT(*) AS has_workflow_enabled_first_three_days_pct,
        has_workflow_enabled_first_week_count / COUNT(*) AS has_workflow_enabled_first_week_pct,
        has_workflow_enabled_first_two_weeks_count / COUNT(*) AS has_workflow_enabled_first_two_weeks_pct,
        has_workflow_enabled_first_month_count / COUNT(*) AS has_workflow_enabled_first_month_pct,
        has_workflow_enabled_first_two_months_count / COUNT(*) AS has_workflow_enabled_first_two_months_pct,
        has_workflow_enabled_first_three_months_count / COUNT(*) AS has_workflow_enabled_first_three_months_pct,
        has_workflow_enabled_first_six_months_count / COUNT(*) AS has_workflow_enabled_first_six_months_pct,
        has_workflow_enabled_first_year_count / COUNT(*) AS has_workflow_enabled_first_year_pct
    FROM shops
    LEFT JOIN enabled_funnel_achievements USING (shop_subdomain)
    GROUP BY 1
),

plan_upgrade_counts AS (
    SELECT
        cohort_month,
        COUNT_IF(NOT (first_plan_upgrade_date IS NULL)) AS ever_paid_plan_count,
        ever_paid_plan_count / COUNT(*) AS ever_paid_plan_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('day', 1, first_installed_at_pt)) AS paid_plan_first_day_count,
        paid_plan_first_day_count / COUNT(*) AS paid_plan_first_day_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('day', 2, first_installed_at_pt)) AS paid_plan_first_two_days_count,
        paid_plan_first_two_days_count / COUNT(*) AS paid_plan_first_two_days_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('day', 3, first_installed_at_pt)) AS paid_plan_first_three_days_count,
        paid_plan_first_three_days_count / COUNT(*) AS paid_plan_first_three_days_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('week', 1, first_installed_at_pt)) AS paid_plan_first_week_count,
        paid_plan_first_week_count / COUNT(*) AS paid_plan_first_week_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('week', 2, first_installed_at_pt)) AS paid_plan_first_two_weeks_count,
        paid_plan_first_two_weeks_count / COUNT(*) AS paid_plan_first_two_weeks_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 1, first_installed_at_pt)) AS paid_plan_first_month_count,
        paid_plan_first_month_count / COUNT(*) AS paid_plan_first_month_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 2, first_installed_at_pt)) AS paid_plan_first_two_months_count,
        paid_plan_first_two_months_count / COUNT(*) AS paid_plan_first_two_months_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 3, first_installed_at_pt)) AS paid_plan_first_three_months_count,
        paid_plan_first_three_months_count / COUNT(*) AS paid_plan_first_three_months_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 6, first_installed_at_pt)) AS paid_plan_first_six_months_count,
        paid_plan_first_six_months_count / COUNT(*) AS paid_plan_first_six_months_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('year', 1, first_installed_at_pt)) AS paid_plan_first_year_count,
        paid_plan_first_year_count / COUNT(*) AS paid_plan_first_year_pct
    FROM shops
    GROUP BY 1
),

shopify_plan_counts AS (
    SELECT
        cohort_month
        {% for plan in ['basic', 'professional', 'shopify_plus', 'unlimited', 'trial' ] %}
        , COUNT_IF(shopify_plan_name = '{{ plan }}') AS shopify_{{ plan }}_plan_count,
        shopify_{{ plan }}_plan_count / COUNT(*) AS shopify_{{ plan }}_plan_pct
        {% endfor %}
    FROM shops
    GROUP BY 1
),


final AS (

    SELECT
        *,
        total_ltv_revenue / NULLIF(cohort_size, 0) AS lifetime_value_installed,
        total_ltv_revenue / NULLIF(has_a_workflow_count, 0) AS lifetime_value_has_a_workflow,
        total_ltv_revenue / NULLIF(has_enabled_a_workflow_count, 0) AS lifetime_value_enabled_workflow,
        total_ltv_revenue / NULLIF(is_activated_count, 0) AS lifetime_value_activated
    FROM workflow_step_counts
    LEFT JOIN workflows_created_time_buckets USING (cohort_month)
    LEFT JOIN workflows_enabled_time_buckets USING (cohort_month)
    LEFT JOIN plan_upgrade_counts USING (cohort_month)
    LEFT JOIN shopify_plan_counts USING (cohort_month)
)

SELECT *
FROM final
WHERE cohort_month <= date_trunc('month', CURRENT_DATE())
ORDER BY cohort_month DESC
