WITH
shops AS (
    SELECT *
    FROM {{ ref('shops') }}
),

workflow_setup_counts AS (
    SELECT
        cohort_week,
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
        cohort_week,
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
        cohort_week,
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
        cohort_week,
        COUNT_IF(NOT (plan_upgrade_date IS NULL)) AS ever_paid_plan_count,
        ever_paid_plan_count / COUNT(*) AS ever_paid_plan_pct,
        COUNT_IF(plan_upgrade_date < DATEADD('day', 1, first_installed_at_pt)) AS paid_plan_first_day_count,
        paid_plan_first_day_count / COUNT(*) AS paid_plan_first_day_pct,
        COUNT_IF(plan_upgrade_date < DATEADD('day', 2, first_installed_at_pt)) AS paid_plan_first_two_days_count,
        paid_plan_first_two_days_count / COUNT(*) AS paid_plan_first_two_days_pct,
        COUNT_IF(plan_upgrade_date < DATEADD('day', 3, first_installed_at_pt)) AS paid_plan_first_three_days_count,
        paid_plan_first_three_days_count / COUNT(*) AS paid_plan_first_three_days_pct,
        COUNT_IF(plan_upgrade_date < DATEADD('week', 1, first_installed_at_pt)) AS paid_plan_first_week_count,
        paid_plan_first_week_count / COUNT(*) AS paid_plan_first_week_pct,
        COUNT_IF(plan_upgrade_date < DATEADD('week', 2, first_installed_at_pt)) AS paid_plan_first_two_weeks_count,
        paid_plan_first_two_weeks_count / COUNT(*) AS paid_plan_first_two_weeks_pct,
        COUNT_IF(plan_upgrade_date < DATEADD('month', 1, first_installed_at_pt)) AS paid_plan_first_month_count,
        paid_plan_first_month_count / COUNT(*) AS paid_plan_first_month_pct,
        COUNT_IF(plan_upgrade_date < DATEADD('month', 2, first_installed_at_pt)) AS paid_plan_first_two_months_count,
        paid_plan_first_two_months_count / COUNT(*) AS paid_plan_first_two_months_pct,
        COUNT_IF(plan_upgrade_date < DATEADD('month', 3, first_installed_at_pt)) AS paid_plan_first_three_months_count,
        paid_plan_first_three_months_count / COUNT(*) AS paid_plan_first_three_months_pct,
        COUNT_IF(plan_upgrade_date < DATEADD('month', 6, first_installed_at_pt)) AS paid_plan_first_six_months_count,
        paid_plan_first_six_months_count / COUNT(*) AS paid_plan_first_six_months_pct,
        COUNT_IF(plan_upgrade_date < DATEADD('year', 1, first_installed_at_pt)) AS paid_plan_first_year_count,
        paid_plan_first_year_count / COUNT(*) AS paid_plan_first_year_pct
    FROM shops
    GROUP BY 1
),

mql_counts AS (
    SELECT
        cohort_week,
        COUNT_IF(is_mql) AS mql_count
    FROM shops
    GROUP BY 1
),

non_mesa_installs AS (
    SELECT
        date_trunc('week', updated_at) AS cohort_week,
        COUNT(*) AS non_mesa_shop_count,
        COUNT_IF(is_mql) AS non_mesa_mql_count
    FROM {{ ref('int_shop_infos') }}
    LEFT OUTER JOIN shops USING (shop_subdomain)
    WHERE shop_subdomain NOT IN (SELECT * FROM {{ ref('staff_subdomains') }})
    GROUP BY 1
),

shopify_plan_counts AS (
    SELECT
        cohort_week
        {% for plan in ['basic', 'professional', 'shopify_plus', 'unlimited', 'trial' ] %},
            COUNT_IF(shopify_plan_name = '{{ plan }}') AS shopify_{{ plan }}_plan_count,
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
        total_ltv_revenue / NULLIF(is_activated_count, 0) AS lifetime_value_activated,

        {# Rest of Constellation Growth #}
        LAG(non_mesa_shop_count) OVER (ORDER BY cohort_week) AS prior_one_week_non_mesa_customer_count,
        1.0 * cohort_size / NULLIF(prior_one_week_non_mesa_customer_count, 0) -1 AS one_week_non_mesa_growth_rate,

        {# Rest of Constellation MQL Growth #}
        LAG(non_mesa_mql_count) OVER (ORDER BY cohort_week) AS prior_one_week_non_mesa_mql_count,
        1.0 * non_mesa_mql_count / NULLIF(prior_one_week_non_mesa_mql_count, 0) - 1 AS one_week_non_mesa_mql_growth_rate,

        {# MESA Install Growth #}
        LAG(cohort_size) OVER (ORDER BY cohort_week) AS prior_one_week_new_customer_count,
        1.0 * cohort_size / NULLIF(prior_one_week_new_customer_count, 0) - 1 AS one_week_mesa_growth_rate,
        1.0 * prior_one_week_non_mesa_customer_count / (1 + NULLIF(one_week_non_mesa_growth_rate, 0)) AS one_week_normalized_customer_count,

        {# MESA MQL Growth #}
        LAG(mql_count) OVER (ORDER BY cohort_week) AS prior_one_week_mql_count,
        1.0 * mql_count / NULLIF(prior_one_week_mql_count, 0) - 1 AS one_week_mql_growth_rate,
        1.0 * mql_count / NULLIF(1 + one_week_non_mesa_mql_growth_rate, 0) AS one_week_normalized_mql_count
    FROM workflow_setup_counts
    LEFT JOIN workflows_created_time_buckets USING (cohort_week)
    LEFT JOIN workflows_enabled_time_buckets USING (cohort_week)
    LEFT JOIN shopify_plan_counts USING (cohort_week)
    LEFT JOIN plan_upgrade_counts USING (cohort_week)
    LEFT JOIN mql_counts USING (cohort_week)
    LEFT JOIN non_mesa_installs USING (cohort_week)
)

SELECT
    *,
    LAG(one_week_normalized_mql_count) OVER (ORDER BY cohort_week) AS prior_one_week_normalized_mql_count,
    1.0 * mql_count / NULLIF(prior_one_week_normalized_mql_count, 0) - 1 AS normalized_growth_rate,
    1.0 * one_week_normalized_mql_count / NULLIF(prior_one_week_normalized_mql_count, 0) - 1 AS double_normalized_growth_rate
FROM final
WHERE cohort_week < date_trunc('week', CURRENT_DATE())
ORDER BY cohort_week DESC
