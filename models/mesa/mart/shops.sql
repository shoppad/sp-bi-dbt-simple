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
        COALESCE(SUM(run_start_count), 0) AS workflow_runs_count,
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

app_install_bookend_times AS (
    {# TODO: Start logging Install and Uninstall events. Transition this to use those. #}
    SELECT
        id AS shop_subdomain,
        {{ pacific_timestamp('APPS_MESA_INSTALLEDAT') }} AS installed_app_at_pt,
        {{ pacific_timestamp('APPS_MESA_UNINSTALLEDAT') }} AS uninstalled_app_at_pt,
        {{ datediff('installed_app_at_pt', 'uninstalled_app_at_pt', 'minute') }} AS minutes_until_uninstall
    FROM {{ source('php_segment', 'users') }}
),


final AS (
    SELECT
        *,
        NOT(activation_date_pt IS NULL) AS is_activated,
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
    LEFT JOIN app_install_bookend_times USING (shop_subdomain)
    WHERE billing_accounts.plan_name IS NOT NULL
)

SELECT * FROM final
