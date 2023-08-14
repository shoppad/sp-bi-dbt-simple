WITH
shops AS (
    SELECT *
    FROM {{ ref('shops') }}
    WHERE is_mql
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
        has_workflow_first_day_count / NULLIF(COUNT(*), 0) AS has_workflow_first_day_pct,
        COUNT_IF(_workflow_first_two_days_count > 0) AS has_workflow_first_two_days_count,
        has_workflow_first_two_days_count / NULLIF(COUNT(*), 0) AS has_workflow_first_two_days_pct,
        COUNT_IF(_workflow_first_three_days_count > 0) AS has_workflow_first_three_days_count,
        has_workflow_first_three_days_count / NULLIF(COUNT(*), 0) AS has_workflow_first_three_days_pct,
        COUNT_IF(_workflow_first_week_count > 0) AS has_workflow_first_week_count,
        has_workflow_first_week_count / NULLIF(COUNT(*), 0) AS has_workflow_first_week_pct,
        COUNT_IF(_workflow_first_two_weeks_count > 0) AS has_workflow_first_two_weeks_count,
        has_workflow_first_two_weeks_count / NULLIF(COUNT(*), 0) AS has_workflow_first_two_weeks_pct,
        COUNT_IF(_workflow_first_month_count > 0) AS has_workflow_first_month_count,
        has_workflow_first_month_count / NULLIF(COUNT(*), 0) AS has_workflow_first_month_pct,
        COUNT_IF(_workflow_first_two_months_count > 0) AS has_workflow_first_two_months_count,
        has_workflow_first_two_months_count / NULLIF(COUNT(*), 0) AS has_workflow_first_two_months_pct,
        COUNT_IF(_workflow_first_three_months_count > 0) AS has_workflow_first_three_months_count,
        has_workflow_first_three_months_count / NULLIF(COUNT(*), 0) AS has_workflow_first_three_months_pct,
        COUNT_IF(_workflow_first_six_months_count > 0) AS has_workflow_first_six_months_count,
        has_workflow_first_six_months_count / NULLIF(COUNT(*), 0) AS has_workflow_first_six_months_pct,
        COUNT_IF(_workflow_first_year_count > 0) AS has_workflow_first_year_count,
        has_workflow_first_year_count / NULLIF(COUNT(*), 0) AS has_workflow_first_year_pct
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

        has_workflow_enabled_first_day_count / NULLIF(COUNT(*), 0) AS has_workflow_enabled_first_day_pct,
        has_workflow_enabled_first_two_days_count / NULLIF(COUNT(*), 0) AS has_workflow_enabled_first_two_days_pct,
        has_workflow_enabled_first_three_days_count / NULLIF(COUNT(*), 0) AS has_workflow_enabled_first_three_days_pct,
        has_workflow_enabled_first_week_count / NULLIF(COUNT(*), 0) AS has_workflow_enabled_first_week_pct,
        has_workflow_enabled_first_two_weeks_count / NULLIF(COUNT(*), 0) AS has_workflow_enabled_first_two_weeks_pct,
        has_workflow_enabled_first_month_count / NULLIF(COUNT(*), 0) AS has_workflow_enabled_first_month_pct,
        has_workflow_enabled_first_two_months_count / NULLIF(COUNT(*), 0) AS has_workflow_enabled_first_two_months_pct,
        has_workflow_enabled_first_three_months_count / NULLIF(COUNT(*), 0) AS has_workflow_enabled_first_three_months_pct,
        has_workflow_enabled_first_six_months_count / NULLIF(COUNT(*), 0) AS has_workflow_enabled_first_six_months_pct,
        has_workflow_enabled_first_year_count / NULLIF(COUNT(*), 0) AS has_workflow_enabled_first_year_pct
    FROM shops
    LEFT JOIN enabled_funnel_achievements USING (shop_subdomain)
    GROUP BY 1
),

plan_upgrade_counts AS (
    SELECT
        cohort_week,
        COUNT_IF(ever_upgraded_to_paid_plan) AS ever_paid_plan_count,
        ever_paid_plan_count / NULLIF(COUNT(*), 0) AS ever_paid_plan_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('day', 1, first_installed_at_pt)) AS paid_plan_first_day_count,
        paid_plan_first_day_count / NULLIF(COUNT(*), 0) AS paid_plan_first_day_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('day', 2, first_installed_at_pt)) AS paid_plan_first_two_days_count,
        paid_plan_first_two_days_count / NULLIF(COUNT(*), 0) AS paid_plan_first_two_days_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('day', 3, first_installed_at_pt)) AS paid_plan_first_three_days_count,
        paid_plan_first_three_days_count / NULLIF(COUNT(*), 0) AS paid_plan_first_three_days_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('week', 1, first_installed_at_pt)) AS paid_plan_first_week_count,
        paid_plan_first_week_count / NULLIF(COUNT(*), 0) AS paid_plan_first_week_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('week', 2, first_installed_at_pt)) AS paid_plan_first_two_weeks_count,
        paid_plan_first_two_weeks_count / NULLIF(COUNT(*), 0) AS paid_plan_first_two_weeks_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 1, first_installed_at_pt)) AS paid_plan_first_month_count,
        paid_plan_first_month_count / NULLIF(COUNT(*), 0) AS paid_plan_first_month_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 2, first_installed_at_pt)) AS paid_plan_first_two_months_count,
        paid_plan_first_two_months_count / NULLIF(COUNT(*), 0) AS paid_plan_first_two_months_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 3, first_installed_at_pt)) AS paid_plan_first_three_months_count,
        paid_plan_first_three_months_count / NULLIF(COUNT(*), 0) AS paid_plan_first_three_months_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('month', 6, first_installed_at_pt)) AS paid_plan_first_six_months_count,
        paid_plan_first_six_months_count / NULLIF(COUNT(*), 0) AS paid_plan_first_six_months_pct,
        COUNT_IF(first_plan_upgrade_date < DATEADD('year', 1, first_installed_at_pt)) AS paid_plan_first_year_count,
        paid_plan_first_year_count / NULLIF(COUNT(*), 0) AS paid_plan_first_year_pct
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

weekly_constellation_installs AS (
    SELECT
        constellation_cohort_week,
        COUNT(stg_constellation_users.*) AS constellation_cohort_size,
        COUNT_IF(stg_constellation_users.is_mql) AS constellation_mql_count
    FROM {{ ref('stg_constellation_users') }}
    LEFT OUTER JOIN shops USING (shop_subdomain)
    WHERE shop_subdomain NOT IN (SELECT * FROM {{ ref('staff_subdomains') }})
    GROUP BY 1
),

constellation_comparison_groups AS (
    SELECT
        constellation_cohort_week AS cohort_week,
        AVG(constellation_cohort_size)
            OVER (ORDER BY constellation_cohort_week ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
            AS avg_constellation_cohort_size,
        AVG(constellation_mql_count)
            OVER (ORDER BY constellation_cohort_week ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
            AS avg_constellation_mql_count,
        avg_constellation_mql_count / NULLIF(avg_constellation_cohort_size, 0)
            AS avg_constellation_mql_pct
    FROM weekly_constellation_installs
),

decorated_constellation_comparison_groups AS (
    SELECT
        *,
        LAG(avg_constellation_cohort_size)
            OVER (ORDER BY cohort_week) AS last_period_avg_constellation_cohort_size,
        LAG(avg_constellation_mql_count)
            OVER (ORDER BY cohort_week) AS last_period_avg_constellation_mql_count,
        avg_constellation_cohort_size / NULLIF(last_period_avg_constellation_cohort_size - 1, 0)
            AS avg_constellation_cohort_size_growth,
        avg_constellation_mql_count / NULLIF(last_period_avg_constellation_mql_count - 1, 0)
            AS avg_constellation_mql_count_growth
    FROM constellation_comparison_groups
),

shopify_plan_counts AS (
    SELECT
        cohort_week
        {% for plan in ['basic', 'professional', 'shopify_plus', 'unlimited', 'trial' ] %},
            COUNT_IF(shopify_plan_name = '{{ plan }}') AS shopify_{{ plan }}_plan_count,
            shopify_{{ plan }}_plan_count / NULLIF(COUNT(*), 0) AS shopify_{{ plan }}_plan_pct
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

        {# MESA Install Growth #}
        LAG(cohort_size) OVER (ORDER BY cohort_week) AS prior_one_week_new_customer_count,
        cohort_size / NULLIF(prior_one_week_new_customer_count- 1, 0) AS mesa_growth_rate,
        cohort_size * NULLIF(1 - avg_constellation_cohort_size_growth, 0) AS normalized_customer_count,
        mesa_growth_rate * NULLIF(1 - avg_constellation_cohort_size_growth, 0) AS normalized_customer_growth,

        {# MESA MQL Growth #}
        LAG(mql_count) OVER (ORDER BY cohort_week) AS prior_one_week_mql_count,
        mql_count / NULLIF(prior_one_week_mql_count- 1, 0)  AS mql_growth_rate,
        mql_count * NULLIF(1 - avg_constellation_mql_count_growth, 0) AS normalized_mql_count,
        mql_growth_rate * NULLIF(1 - avg_constellation_mql_count_growth, 0) AS normalized_mql_growth

        {# MESA MQL Growth #}
    FROM workflow_setup_counts
    LEFT JOIN workflows_created_time_buckets USING (cohort_week)
    LEFT JOIN workflows_enabled_time_buckets USING (cohort_week)
    LEFT JOIN shopify_plan_counts USING (cohort_week)
    LEFT JOIN plan_upgrade_counts USING (cohort_week)
    LEFT JOIN mql_counts USING (cohort_week)
    LEFT JOIN decorated_constellation_comparison_groups USING (cohort_week)
)

SELECT
    *
FROM final
WHERE cohort_week < date_trunc('week', CURRENT_DATE())
ORDER BY cohort_week DESC
