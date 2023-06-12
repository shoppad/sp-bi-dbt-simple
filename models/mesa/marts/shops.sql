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
        COUNT(DISTINCT workflows.template_name) AS templates_installed_count
    FROM shops
    LEFT JOIN workflows USING (shop_subdomain)
    GROUP BY
        1
),


workflow_run_counts AS (
    SELECT
        shop_subdomain,
        COALESCE(COUNT(DISTINCT workflow_id), 0) AS unique_workflows_attempted_count,
        COALESCE(COUNT(workflow_runs.workflow_run_id), 0) AS workflow_runs_attempted_count
    FROM shops
    LEFT JOIN {{ ref('workflow_runs') }} USING (shop_subdomain)
    GROUP BY 1
),

successful_workflow_run_counts AS (
    SELECT
        shops.shop_subdomain,
        COALESCE(COUNT(workflow_run_id), 0) AS workflow_run_success_count,
        COALESCE(COUNT(DISTINCT workflow_id), 0) AS unique_workflows_successfully_run_count
    FROM shops
    LEFT JOIN {{ ref('workflow_runs') }}
        ON
            shops.shop_subdomain = workflow_runs.shop_subdomain
            AND workflow_runs.run_status = 'success'
    GROUP BY 1
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
        SUM(duration_in_s) / 60 AS minutes_using_app
    FROM {{ ref('segment_web_page_views__sessionized') }}
    LEFT JOIN {{ ref('segment_web_sessions') }} USING (session_id)
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
        COALESCE(workflow_runs_rolling_year_count, 0) AS workflow_runs_rolling_year_count,
        COALESCE(income_rolling_thirty_day_total, 0) AS income_rolling_thirty_day_total,
        COALESCE(income_rolling_year_total, 0) AS income_rolling_year_total
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
        achieved_at_pt AS max_funnel_step_achieved_at_pt,
        step_order AS max_funnel_step,
        CASE
            WHEN activation_date_pt IS NOT NULL
                THEN '7-Activated'
            ELSE
                (step_order || '-' || name)
        END AS max_funnel_step_name,
        COALESCE(step_order, 0) >= 3 AS has_a_workflow,
        COALESCE(step_order, 0) >= 6 AS has_enabled_a_workflow
    FROM shops
    LEFT JOIN {{ ref('int_mesa_shop_funnel_achievements') }} USING (shop_subdomain)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY step_order DESC) = 1
),

total_ltv_revenue AS (
    SELECT
        shop_subdomain,
        COALESCE(SUM(inc_amount), 0) AS total_ltv_revenue
    FROM shops
    LEFT JOIN {{ ref('mesa_shop_days') }} USING (shop_subdomain)
    GROUP BY 1
),

shop_infos AS (
    SELECT *
        EXCLUDE (
            updated_at,
            shopify_createdat,
            analytics_gmv,
            shopify_plandisplayname,
            shopify_inactiveat,
            analytics_orders,
            shopify_planname
        )
    FROM {{ ref('int_shop_infos') }}
),

cohort_average_current_shop_gmv AS (
    SELECT AVG(shopify_shop_gmv_current_total_usd) AS avg_current_gmv_usd
    FROM {{ ref('int_shops') }}
),

cohort_average_initial_shop_gmv AS (
    SELECT
        cohort_month,
        AVG(shopify_shop_gmv_initial_total_usd) AS avg_initial_gmv_usd
    FROM {{ ref('int_shops') }}
    GROUP BY 1
),

email_open_details AS (
    SELECT
        shop_subdomain,
        MIN(CASE WHEN email_type = 'broadcast' THEN opened_at_pt ELSE NULL END) AS first_broadcast_email_open_at_pt,
        MAX(CASE WHEN email_type = 'broadcast' THEN opened_at_pt ELSE NULL END) AS last_broadcast_email_open_at_pt,
        COALESCE(
            COUNT(DISTINCT CASE WHEN email_type = 'broadcast' THEN email_id ELSE NULL END),
            0
            ) AS broadcast_email_opens_count,
        broadcast_email_opens_count > 0 AS has_opened_broadcast_email,

        MIN(CASE WHEN email_type = 'journey' THEN opened_at_pt ELSE NULL END) AS first_journey_email_open_at_pt,
        MAX(CASE WHEN email_type = 'journey' THEN opened_at_pt ELSE NULL END) AS last_journey_email_open_at_pt,
        COALESCE(
            COUNT(DISTINCT CASE WHEN email_type = 'journey' THEN email_id ELSE NULL END),
            0
            ) AS journey_email_opens_count,
        journey_email_opens_count > 0 AS has_opened_journey_email
    FROM shops
    LEFT JOIN {{ ref('stg_email_opens') }} USING (shop_subdomain)
    GROUP BY 1
),

email_click_details AS (
    SELECT
        shop_subdomain,
        MIN(CASE WHEN email_type = 'broadcast' THEN clicked_at_pt ELSE NULL END) AS first_broadcast_email_clicked_at_pt,
        MAX(CASE WHEN email_type = 'broadcast' THEN clicked_at_pt ELSE NULL END) AS last_broadcast_email_clicked_at_pt,
        COALESCE(
            COUNT(DISTINCT CASE WHEN email_type = 'broadcast' THEN email_id ELSE NULL END),
            0
            ) AS broadcast_email_click_count,
        broadcast_email_click_count > 0 AS has_clicked_broadcast_email,

        MIN(CASE WHEN email_type = 'journey' THEN clicked_at_pt ELSE NULL END) AS first_journey_email_clicked_at_pt,
        MAX(CASE WHEN email_type = 'journey' THEN clicked_at_pt ELSE NULL END) AS last_journey_email_clicked_at_pt,
        COALESCE(
            COUNT(DISTINCT CASE WHEN email_type = 'journey' THEN email_id ELSE NULL END),
            0
            ) AS journey_email_click_count,
        journey_email_click_count > 0 AS has_clicked_journey_email
    FROM shops
    LEFT JOIN {{ ref('stg_email_clicks') }} USING (shop_subdomain)
    GROUP BY 1
),

email_conversion_details AS (
    SELECT
        shop_subdomain,
        MIN(CASE WHEN email_type = 'broadcast' THEN converted_at_pt ELSE NULL END) AS first_broadcast_email_converted_at_pt,
        MAX(CASE WHEN email_type = 'broadcast' THEN converted_at_pt ELSE NULL END) AS last_broadcast_email_converted_at_pt,
        COALESCE(
            COUNT(DISTINCT CASE WHEN email_type = 'broadcast' THEN email_id ELSE NULL END),
            0
            ) AS broadcast_email_conversion_count,
        broadcast_email_conversion_count > 0 AS has_converted_via_broadcast_email,

        MIN(CASE WHEN email_type = 'journey' THEN converted_at_pt ELSE NULL END) AS first_journey_email_converted_at_pt,
        MAX(CASE WHEN email_type = 'journey' THEN converted_at_pt ELSE NULL END) AS last_journey_email_converted_at_pt,
        COALESCE(
            COUNT(DISTINCT CASE WHEN email_type = 'journey' THEN email_id ELSE NULL END),
            0
            ) AS journey_email_conversion_count,
        journey_email_conversion_count > 0 AS has_converted_via_journey_email
    FROM shops
    LEFT JOIN {{ ref('stg_email_conversions') }} USING (shop_subdomain)
    GROUP BY 1
),

final AS (
    SELECT
        * EXCLUDE (has_had_launch_session, avg_current_gmv_usd, avg_initial_gmv_usd),
        NOT(activation_date_pt IS NULL) AS is_activated,
        IFF(is_activated, 'activated', 'onboarding') AS funnel_phase,
        {{ dbt.datediff('first_installed_at_pt::DATE', 'activation_date_pt', 'days') }} AS days_to_activation,
        COALESCE(has_had_launch_session, NOT launch_session_date IS NULL) AS has_had_launch_session,
        {{ dbt.datediff('launch_session_date', 'activation_date_pt', 'days') }} AS days_from_launch_session_to_activation,
        shopify_shop_gmv_current_total_usd / NULLIF(avg_current_gmv_usd, 0) - 1 AS shopify_shop_gmv_current_cohort_avg_percent,
        shopify_shop_gmv_initial_total_usd / NULLIF(avg_initial_gmv_usd, 0) - 1 AS shopify_shop_gmv_initial_cohort_avg_percent,
        CASE
            WHEN shopify_shop_gmv_current_total_usd < 100 THEN 100
            WHEN shopify_shop_gmv_current_total_usd < 1000 THEN 1000
            WHEN shopify_shop_gmv_current_total_usd < 10000 THEN 10000
            WHEN shopify_shop_gmv_current_total_usd < 50000 THEN 50000
            WHEN shopify_shop_gmv_current_total_usd < 100000 THEN 100000
            WHEN shopify_shop_gmv_current_total_usd < 250000 THEN 250000
            WHEN shopify_shop_gmv_current_total_usd < 500000 THEN 500000
            WHEN shopify_shop_gmv_current_total_usd < 750000 THEN 750000
            WHEN shopify_shop_gmv_current_total_usd < 1000000 THEN 1000000
            WHEN shopify_shop_gmv_current_total_usd < 2000000 THEN 2000000
            WHEN shopify_shop_gmv_current_total_usd < 5000000 THEN 5000000
            WHEN shopify_shop_gmv_current_total_usd < 10000000 THEN 10000000
            WHEN shopify_shop_gmv_current_total_usd < 20000000 THEN 20000000
            WHEN shopify_shop_gmv_current_total_usd < 50000000 THEN 50000000
            WHEN shopify_shop_gmv_current_total_usd < 100000000 THEN 100000000
            WHEN shopify_shop_gmv_current_total_usd < 200000000 THEN 200000000
            WHEN shopify_shop_gmv_current_total_usd < 500000000 THEN 500000000
            WHEN shopify_shop_gmv_current_total_usd < 1000000000 THEN 1000000000
        END AS shopify_shop_gmv_current_total_tier,

        'https://www.theshoppad.com/homeroom.theshoppad.com/admin/backdoor/'
        || shop_subdomain
        || '/mesa' AS backdoor_url,
        'https://insights.hotjar.com/sites/1547357/'
        || 'workspaces/1288874/playbacks/list?'
        || 'filters=%7B%22AND%22:%5B%7B%22DAYS_AGO%22:%7B%22created%22:365%7D%7D,'
        || '%7B%22EQUAL%22:%7B%22user_attributes.str.user_id%22:%22'
        || shop_subdomain
        || '%22%7D%7D%5D%7D' AS hotjar_url,
        CASE
            WHEN store_leads_estimated_monthly_sales < 1000 THEN 'A-Under $1,000'
            WHEN store_leads_estimated_monthly_sales < 5000 THEN 'B-$1,000-$5,000'
            WHEN store_leads_estimated_monthly_sales < 10000 THEN 'C-$5,000-$10,000'
            WHEN store_leads_estimated_monthly_sales < 25000 THEN 'D-$10,000-$25,000'
            WHEN store_leads_estimated_monthly_sales < 50000 THEN 'E-$25,000-$50,000'
            WHEN store_leads_estimated_monthly_sales < 100000 THEN 'F-$50,000-$100,000'
            WHEN store_leads_estimated_monthly_sales < 250000 THEN 'G-$100,000-$250,000'
            WHEN store_leads_estimated_monthly_sales < 500000 THEN 'H-$250,000-$500,000'
            WHEN store_leads_estimated_monthly_sales < 1000000 THEN 'I-$500,000-$1,000,000'
            WHEN store_leads_estimated_monthly_sales < 2500000 THEN 'J-$1,000,000-$2,500,000'
            ELSE 'K-$2,500,000+'
        END AS store_leads_estimated_monthly_sales_bucket
    FROM shops
    LEFT JOIN billing_accounts USING (shop_subdomain)
    LEFT JOIN price_per_actions USING (shop_subdomain)
    LEFT JOIN workflow_counts USING (shop_subdomain)
    LEFT JOIN workflow_run_counts USING (shop_subdomain)
    LEFT JOIN successful_workflow_run_counts USING (shop_subdomain)
    LEFT JOIN app_pageview_bookend_times USING (shop_subdomain)
    LEFT JOIN current_rolling_counts USING (shop_subdomain)
    LEFT JOIN install_sources USING (shop_subdomain)
    LEFT JOIN max_funnel_steps USING (shop_subdomain)
    LEFT JOIN total_ltv_revenue USING (shop_subdomain)
    LEFT JOIN constellation_app_presences USING (shop_subdomain)
    LEFT JOIN shop_infos USING (shop_subdomain)
    LEFT JOIN cohort_average_current_shop_gmv
    LEFT JOIN cohort_average_initial_shop_gmv USING (cohort_month)
    LEFT JOIN email_open_details USING (shop_subdomain)
    LEFT JOIN email_click_details USING (shop_subdomain)
    LEFT JOIN email_conversion_details USING (shop_subdomain)
    WHERE billing_accounts.plan_name IS NOT NULL
)

SELECT * FROM final
