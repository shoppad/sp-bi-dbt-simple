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

workflow_counts AS (
    SELECT
        shop_subdomain,
        COUNT(*) AS workflows_created_count,
        COUNT_IF(is_enabled) AS workflows_enabled_count,
        COUNT_IF(first_successful_run_at_pt IS NOT NULL) AS workflows_successfully_run_count,
        COUNT(DISTINCT template_name) AS templates_installed_count
    FROM shops
    LEFT JOIN {{ ref('workflows') }} USING (shop_subdomain)
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

final AS (
    SELECT
        *,
        NOT(activation_date_pt IS NULL) AS is_activated,
        {{ datediff('first_installed_at_pt::DATE', 'activation_date_pt', 'days') }} AS days_to_activation,
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
    WHERE billing_accounts.plan_name IS NOT NULL
)

SELECT * FROM final
