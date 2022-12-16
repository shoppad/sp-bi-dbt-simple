WITH
shops AS (
    SELECT *
    FROM {{ ref('int_shops') }}
),

billing_accounts AS (
    SELECT * FROM {{ ref('stg_mesa_billing_accounts') }}
),

price_per_actions AS (
    SELECT
        shop_subdomain,
        "value" AS price_per_action
    FROM {{ ref('stg_shop_entitlements') }}
    WHERE "name" = 'price_per_action'
),

workflows AS (
    SELECT * FROM {{ ref('workflows') }}

),

workflow_counts AS (
    SELECT
        shop_subdomain,
        COUNT(DISTINCT workflows.workflow_id) AS workflows_current_count,
        COUNT_IF(workflows.is_enabled) AS workflows_enabled_current_count,
        COUNT_IF(workflows.first_successful_run_at_pt IS NOT NULL) AS workflows_successfully_run_count, {# TODO: This needs to be based on events or workflow_runs as Workflows will get deleted. #}
        COUNT(DISTINCT workflows.template_name) AS templates_installed_count
    FROM shops
    LEFT JOIN workflows USING (shop_subdomain)
    GROUP BY
        1
),

workflow_run_counts AS (
    SELECT
        shop_subdomain,
        COALESCE(SUM(run_attempt_count), 0) AS workflow_runs_count,
        COALESCE(SUM(run_success_count), 0) AS workflow_run_success_count
    FROM shops
    LEFT JOIN {{ ref('workflows') }} USING (shop_subdomain)
    GROUP BY
        1
),

constellation_app_presences AS (
    SELECT *
    FROM {{ ref('int_mesa_constellation_relationships') }}
),

app_pageview_bookend_times AS (
    SELECT
        user_id AS shop_subdomain,
        {{ pacific_timestamp('MIN(tstamp)') }} AS first_seen_in_app_at_pt,
        {{ pacific_timestamp('MAX(tstamp)') }} AS last_seen_in_app_at_pt,
        {{ datediff('first_seen_in_app_at_pt', 'last_seen_in_app_at_pt', 'minute') }} AS minutes_using_app
    FROM {{ ref('segment_web_page_views__sessionized') }}
    WHERE page_url_host = 'app.getmesa.com'
    GROUP BY 1
),
{# TODO: Change minutes_using_app to SUM() all session lengths. #}

yesterdays AS (
    SELECT *
    FROM {{ ref('mesa_shop_days') }}
    WHERE dt = {{ pacific_timestamp('CURRENT_DATE()') }}::date - INTERVAL '1 day'
),

current_rolling_counts AS (
    SELECT
        shop_subdomain,
        COALESCE(workflow_runs_rolling_thirty_day_count, 0) AS workflow_runs_rolling_thirty_day_count,
        COALESCE(workflow_runs_rolling_year_count, 0) AS workflow_runs_rolling_year_count
    FROM shops
    LEFT JOIN yesterdays USING (shop_subdomain)
),

install_sources AS (
    SELECT *
    FROM {{ ref('int_shop_install_sources') }}
),

max_funnel_steps AS (
    SELECT
        shop_subdomain,
        name AS max_funnel_step_name,
        achieved_at_pt AS max_funnel_step_achieved_at_pt,
        step_order AS max_funnel_step,
        COALESCE(step_order, 0) >= 3 AS has_a_workflow,
        COALESCE(step_order, 0) >=6 AS has_enabled_a_workflow
    FROM shops
    LEFT JOIN {{ ref('int_mesa_shop_funnel_achievements') }} USING (shop_subdomain)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY step_order DESC) = 1
),

total_revenue AS (
    SELECT
        shop_subdomain,
        COALESCE(SUM(inc_amount), 0) AS total_revenue
    FROM shops
    LEFT JOIN {{ ref('mesa_shop_days') }} USING (shop_subdomain)
    GROUP BY 1
),

final AS (
    SELECT
        * EXCLUDE (has_had_launch_session),
        NOT(activation_date_pt IS NULL) AS is_activated,
        IFF(is_activated, 'activated', 'onboarding') AS funnel_phase,

        {{ dbt.datediff('first_installed_at_pt::DATE', 'activation_date_pt', 'days') }} AS days_to_activation,
        IFNULL(has_had_launch_session, NOT(launch_session_date IS NULL)) AS has_had_launch_session,
        {{ dbt.datediff('launch_session_date', 'activation_date_pt', 'days') }} AS days_from_launch_session_to_activation,
        CASE
            WHEN revenue_current_total_usd < 100 THEN 100
            WHEN revenue_current_total_usd < 1000 THEN 1000
            WHEN revenue_current_total_usd < 10000 THEN 10000
            WHEN revenue_current_total_usd < 50000 THEN 50000
            WHEN revenue_current_total_usd < 100000 THEN 100000
            WHEN revenue_current_total_usd < 250000 THEN 250000
            WHEN revenue_current_total_usd < 500000 THEN 500000
            WHEN revenue_current_total_usd < 750000 THEN 750000
            WHEN revenue_current_total_usd < 1000000 THEN 1000000
            WHEN revenue_current_total_usd < 2000000 THEN 2000000
            WHEN revenue_current_total_usd < 5000000 THEN 5000000
            WHEN revenue_current_total_usd < 10000000 THEN 10000000
            WHEN revenue_current_total_usd < 20000000 THEN 20000000
            WHEN revenue_current_total_usd < 50000000 THEN 50000000
            WHEN revenue_current_total_usd < 100000000 THEN 100000000
            WHEN revenue_current_total_usd < 200000000 THEN 200000000
            WHEN revenue_current_total_usd < 500000000 THEN 500000000
            WHEN revenue_current_total_usd < 1000000000 THEN 1000000000
        END AS revenue_current_total_tier,

        'https://www.theshoppad.com/homeroom.theshoppad.com/admin/backdoor/' ||
            shop_subdomain ||
            '/mesa' AS backdoor_url,
        'https://insights.hotjar.com/sites/1547357/' ||
            'workspaces/1288874/playbacks/list?' ||
            'filters=%7B%22AND%22:%5B%7B%22DAYS_AGO%22:%7B%22created%22:365%7D%7D,' ||
            '%7B%22EQUAL%22:%7B%22user_attributes.str.user_id%22:%22' ||
            shop_subdomain ||
            '%22%7D%7D%5D%7D' AS hotjar_url
    FROM shops
    LEFT JOIN billing_accounts USING (shop_subdomain)
    LEFT JOIN price_per_actions USING (shop_subdomain)
    LEFT JOIN workflow_counts USING (shop_subdomain)
    LEFT JOIN workflow_run_counts USING (shop_subdomain)
    LEFT JOIN app_pageview_bookend_times USING (shop_subdomain)
    LEFT JOIN current_rolling_counts USING (shop_subdomain)
    LEFT JOIN install_sources USING (shop_subdomain)
    LEFT JOIN max_funnel_steps USING (shop_subdomain)
    LEFT JOIN total_revenue USING (shop_subdomain)
    LEFT JOIN constellation_app_presences USING (shop_subdomain)
    WHERE billing_accounts.plan_name IS NOT NULL
)

SELECT * FROM final
