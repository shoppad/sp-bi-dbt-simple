with

shops AS (SELECT * FROM {{ ref("int_shops") }}),

billing_accounts AS (SELECT * FROM {{ ref("stg_mesa_billing_accounts") }}),

support_entitlements AS (
    SELECT *
    FROM {{ ref("stg_shop_entitlements") }}
    WHERE attribute_name = 'support'
),

csm_support AS (
    SELECT shop_subdomain, COALESCE(attribute_value = 'csm', FALSE) AS has_csm_support
    FROM shops
    LEFT JOIN support_entitlements USING (shop_subdomain)
),

price_per_actions AS (
    SELECT shop_subdomain, attribute_value AS price_per_action
    FROM {{ ref("stg_shop_entitlements") }}
    WHERE attribute_name = 'price_per_action'
),

workflows AS (SELECT * FROM {{ ref("workflows") }} WHERE is_deleted = FALSE),

workflow_counts AS (
    SELECT
        shop_subdomain,
        COUNT_IF(workflows.step_count > 1) AS workflows_current_count,
        COUNT_IF(
            workflows.step_count > 1 AND workflows.is_enabled
        ) AS workflows_enabled_current_count,
        COUNT(DISTINCT workflows.template_name) AS templates_installed_count,
        COUNT_IF(workflows.has_pro_app AND workflows.is_enabled)
        > 0 AS is_using_pro_apps,
        COALESCE(
            sum(workflows.test_attempt_count) > 0, FALSE
        ) AS has_attempted_a_test,
        COALESCE(
            sum(workflows.test_success_count) > 0, FALSE
        ) AS has_successfully_run_a_test,
        COALESCE(MAX(workflows.is_puc), FALSE) AS has_puc_workflow
    FROM shops
    LEFT JOIN workflows USING (shop_subdomain)
    GROUP BY 1
),

int_shop_integration_app_rows AS (
    SELECT
        shop_subdomain,
        COALESCE(
            COUNT(distinct integration_app), 0
        ) AS integration_apps_enabled_count,
        COALESCE(COUNT_IF(is_pro_app), 0) AS pro_apps_enabled_count
    FROM shops
    LEFT JOIN {{ ref("int_shop_integration_app_rows") }} USING (shop_subdomain)
    GROUP BY 1
),

workflow_run_counts AS (
    SELECT
        shop_subdomain,
        COALESCE(
            COUNT(distinct workflow_id), 0
        ) AS unique_workflows_attempted_count,
        COALESCE(
            COUNT(workflow_runs.workflow_run_id), 0
        ) AS workflow_runs_attempted_count,
        COALESCE(COUNT_IF(workflow_runs.is_stop), 0) AS workflow_runs_stop_count,
        COALESCE(COUNT_IF(workflow_runs.is_failure), 0) AS workflow_runs_fail_count
    FROM shops
    LEFT JOIN {{ ref("workflow_runs") }} USING (shop_subdomain)
    GROUP BY 1
),

successful_workflow_run_counts AS (
    SELECT
        shops.shop_subdomain,
        COALESCE(COUNT(workflow_run_id), 0) AS workflow_run_success_count,
        COALESCE(
            COUNT(distinct workflow_id), 0
        ) AS unique_workflows_successfully_run_count
    FROM shops
    LEFT JOIN
        {{ ref("workflow_runs") }}
        on shops.shop_subdomain = workflow_runs.shop_subdomain
        AND workflow_runs.run_status = 'success'
    GROUP BY 1
),

app_pageview_bookend_times AS (
    SELECT
        user_id AS shop_subdomain,
        {{ pacific_timestamp("MIN(tstamp)") }} AS first_seen_in_app_at_pt,
        {{ pacific_timestamp("MAX(tstamp)") }} AS last_seen_in_app_at_pt,
        sum(duration_in_s) / 60 AS minutes_using_app
    FROM {{ ref("segment_web_page_views__sessionized") }}
    LEFT JOIN {{ ref("segment_web_sessions") }} USING (session_id)
    WHERE page_url_host = 'app.getmesa.com'
    GROUP BY 1
),

yesterdays AS (
    SELECT *
    FROM {{ ref("mesa_shop_days") }}
    WHERE dt = {{ pacific_timestamp("CURRENT_TIMESTAMP()") }}::date - interval '1 day'
),

current_rolling_counts AS (
    SELECT
        shop_subdomain,
        COALESCE(
            workflow_run_attempt_rolling_thirty_day_count, 0
        ) AS workflow_run_attempt_rolling_thirty_day_count,
        COALESCE(
            workflow_run_success_rolling_thirty_day_count, 0
        ) AS workflow_run_success_rolling_thirty_day_count,
        COALESCE(
            workflow_run_failure_rolling_thirty_day_count, 0
        ) AS workflow_run_failure_rolling_thirty_day_count,
        COALESCE(
            workflow_run_stop_rolling_thirty_day_count, 0
        ) AS workflow_run_stop_rolling_thirty_day_count,
        COALESCE(
            workflow_run_attempt_rolling_year_count, 0
        ) AS workflow_run_attempt_rolling_year_count,
        COALESCE(
            workflow_run_success_rolling_year_count, 0
        ) AS workflow_run_success_rolling_year_count,
        COALESCE(
            workflow_run_failure_rolling_year_count, 0
        ) AS workflow_run_failure_rolling_year_count,
        COALESCE(
            workflow_run_stop_rolling_year_count, 0
        ) AS workflow_run_stop_rolling_year_count,
        COALESCE(
            income_rolling_thirty_day_total, 0
        ) AS income_rolling_thirty_day_total,
        COALESCE(income_rolling_year_total, 0) AS income_rolling_year_total,
        COALESCE(
            total_workflow_steps_rolling_thirty_day_count, 0
        ) AS total_workflow_steps_rolling_thirty_day_count,
        COALESCE(
            input_step_rolling_thirty_day_count, 0
        ) AS input_step_rolling_thirty_day_count,
        COALESCE(
            output_step_rolling_thirty_day_count, 0
        ) AS output_step_rolling_thirty_day_count,
        COALESCE(inc_amount, 0) AS yesterdays_inc_amount
    FROM shops
    LEFT JOIN yesterdays USING (shop_subdomain)
),

simple_shop_attribution AS (
    SELECT
        *
    FROM {{ ref('int_simplified_shop_attribution') }}
),

max_funnel_steps AS (
    SELECT
        shop_subdomain,
        achieved_at_pt AS max_funnel_step_achieved_at_pt,
        step_order AS max_funnel_step,
        CASE
            WHEN activation_date_pt IS NOT NULL
            THEN '7-Activated'
            ELSE (step_order || '-' || name)
        end AS max_funnel_step_name,
        COALESCE(step_order, 0) >= 3 AS has_a_workflow,
        COALESCE(step_order, 0) >= 4 AS has_saved_a_workflow,
        COALESCE(step_order, 0) >= 6 AS has_enabled_a_workflow
    FROM shops
    LEFT JOIN {{ ref("int_mesa_shop_funnel_achievements") }} USING (shop_subdomain)
    qualify
        row_number() over (partition by shop_subdomain order by step_order desc) = 1
),

total_ltv_revenue AS (
    SELECT shop_subdomain, COALESCE(sum(inc_amount), 0) AS total_ltv_revenue
    FROM shops
    LEFT JOIN {{ ref("mesa_shop_days") }} USING (shop_subdomain)
    GROUP BY 1
),

shop_infos AS (
    SELECT
        * exclude (
            updated_at,
            shopify_createdat,
            analytics_gmv,
            shopify_plandisplayname,
            shopify_inactiveat,
            analytics_orders,
            shopify_planname
        )
    FROM {{ ref("int_shop_infos") }}
),

cohort_average_current_shop_gmv AS (
    SELECT avg(shopify_shop_gmv_current_total_usd) AS avg_current_gmv_usd
    FROM {{ ref("int_shops") }}
),

cohort_average_initial_shop_gmv AS (
    SELECT
        cohort_month, avg(shopify_shop_gmv_initial_total_usd) AS avg_initial_gmv_usd
    FROM {{ ref("int_shops") }}
    GROUP BY 1
),

last_thirty_days AS (
    SELECT *
    FROM {{ ref("mesa_shop_days") }}
    WHERE dt >= current_date - interval '30 day' AND inc_amount > 0
),

thirty_day_revenue AS (
    SELECT
        shop_subdomain,
        COALESCE(avg(daily_usage_revenue), 0) AS average_daily_usage_revenue,
        COALESCE(avg(cast(inc_amount AS float)), 0) AS average_daily_revenue,
        COALESCE(avg(cast(daily_plan_revenue AS float)), 0) AS average_plan_revenue,
        (average_plan_revenue * 30) AS projected_mrr,
        COALESCE(sum(inc_amount), 0) AS total_thirty_day_revenue
    FROM shops
    LEFT JOIN last_thirty_days USING (shop_subdomain)
    GROUP BY 1
),

email_open_details AS (
    SELECT
        shop_subdomain,
        MIN(
            CASE WHEN email_type = 'broadcast' THEN opened_at_pt END
        ) AS first_broadcast_email_open_at_pt,
        MAX(
            CASE WHEN email_type = 'broadcast' THEN opened_at_pt END
        ) AS last_broadcast_email_open_at_pt,
        COALESCE(
            COUNT(
                DISTINCT CASE
                    WHEN email_type = 'broadcast' then email_id
                END
            ),
            0
        ) AS broadcast_email_opens_count,
        broadcast_email_opens_count > 0 AS has_opened_broadcast_email,

        MIN(
            CASE WHEN email_type = 'journey' then opened_at_pt end
        ) AS first_journey_email_open_at_pt,
        MAX(
            CASE WHEN email_type = 'journey' then opened_at_pt end
        ) AS last_journey_email_open_at_pt,
        COALESCE(
            COUNT(
                distinct case
                    when email_type = 'journey' then email_id
                end
            ),
            0
        ) AS journey_email_opens_count,
        journey_email_opens_count > 0 AS has_opened_journey_email
    FROM shops
    LEFT JOIN {{ ref("stg_email_opens") }} USING (shop_subdomain)
    GROUP BY 1
),

email_click_details AS (
    SELECT
        shop_subdomain,
        MIN(
            CASE WHEN email_type = 'broadcast' THEN clicked_at_pt END
        ) AS first_broadcast_email_clicked_at_pt,
        MAX(
            CASE WHEN email_type = 'broadcast' THEN clicked_at_pt END
        ) AS last_broadcast_email_clicked_at_pt,
        COALESCE(
            COUNT(
                DISTINCT CASE
                    WHEN email_type = 'broadcast' THEN email_id
                END
            ),
            0
        ) AS broadcast_email_click_count,
        broadcast_email_click_count > 0 AS has_clicked_broadcast_email,

        MIN(
            CASE WHEN email_type = 'journey' THEN clicked_at_pt END
        ) AS first_journey_email_clicked_at_pt,
        MAX(
            CASE WHEN email_type = 'journey' THEN clicked_at_pt END
        ) AS last_journey_email_clicked_at_pt,
        COALESCE(
            COUNT(
                distinct case
                    when email_type = 'journey' then email_id
                end
            ),
            0
        ) AS journey_email_click_count,
        journey_email_click_count > 0 AS has_clicked_journey_email
    FROM shops
    LEFT JOIN {{ ref("stg_email_clicks") }} USING (shop_subdomain)
    GROUP BY 1
),

email_conversion_details AS (
    SELECT
        shop_subdomain,
        MIN(
            CASE WHEN email_type = 'broadcast' then converted_at_pt end
        ) AS first_broadcast_email_converted_at_pt,
        MAX(
            CASE WHEN email_type = 'broadcast' then converted_at_pt end
        ) AS last_broadcast_email_converted_at_pt,
        COALESCE(
            COUNT(
                distinct case
                    when email_type = 'broadcast' then email_id
                end
            ),
            0
        ) AS broadcast_email_conversion_count,
        broadcast_email_conversion_count > 0 AS has_converted_via_broadcast_email,

        MIN(
            CASE WHEN email_type = 'journey' then converted_at_pt end
        ) AS first_journey_email_converted_at_pt,
        MAX(
            CASE WHEN email_type = 'journey' then converted_at_pt end
        ) AS last_journey_email_converted_at_pt,
        COALESCE(
            COUNT(
                distinct case
                    when email_type = 'journey' then email_id
                end
            ),
            0
        ) AS journey_email_conversion_count,
        journey_email_conversion_count > 0 AS has_converted_via_journey_email
    FROM shops
    LEFT JOIN {{ ref("stg_email_conversions") }} USING (shop_subdomain)
    GROUP BY 1
),

email_unsubscribe_details AS (
    SELECT
        shop_subdomain,
        email_unsubscribe_email_type,
        email_unsubscribe_email_name,
        COALESCE(
            email_unsubscribe_email_type is not NULL, FALSE
        ) AS has_unsubscribed_from_email
    FROM shops
    LEFT JOIN {{ ref("stg_email_unsubscribes") }} USING (shop_subdomain)
    qualify
        row_number() over (
            partition by shop_subdomain order by __hevo__ingested_at desc
        )
        = 1
),

first_workflow_keys AS (SELECT * FROM {{ ref("int_first_workflow_keys") }}),

max_workflow_steps AS (
    SELECT
        shop_subdomain,
        COALESCE(MAX(step_count), 0) AS max_workflow_steps,
        COALESCE(
            MAX(step_count_with_deleted), 0
        ) AS max_workflow_steps_with_deleted,
        COALESCE(MAX(step_count) >= 2, FALSE) AS has_a_workflow
    FROM shops
    LEFT JOIN {{ ref("workflows") }} USING (shop_subdomain)
    GROUP BY 1
),

workflow_source_destination_pairs AS (
    SELECT
        shop_subdomain,
        NULLIF(
            LISTAGG(DISTINCT source_destination_pair, ',') WITHIN GROUP (
                ORDER BY source_destination_pair ASC
            ),
            ''
        ) AS source_destination_pairs_list
    FROM workflows
    GROUP BY 1
),

plan_change_chains AS (
    SELECT
        shop_subdomain,
        COUNT(distinct plan) AS plan_change_count,
        LISTAGG(
            CONCAT(
                IFF(previous_price IS NULL OR previous_price <= price, '↑:', '↓:'),
                planid,
                ':$',
                price
            ),
            ' • '
        ) WITHIN GROUP (ORDER BY changed_at_pt asc) AS plan_change_chain
    FROM shops
    LEFT JOIN {{ ref("stg_mesa_plan_changes") }} USING (shop_subdomain)
    GROUP BY 1
),

last_plan_prices AS (
    SELECT
        shop_subdomain,
        round(max_by(daily_plan_revenue, dt) * 30) AS last_plan_price
    FROM {{ ref("int_shop_calendar") }}
    WHERE daily_plan_revenue > 0
    GROUP BY 1
),

first_newsletter_deliveries AS (
    SELECT * FROM {{ ref("int_first_newsletter_deliveries") }}
),

first_journey_deliveries AS (
    SELECT * FROM {{ ref("int_first_journey_deliveries") }}
),

inc_amount_days_and_day_befores AS (
    SELECT
        shop_subdomain,
        dt,
        inc_amount,
        is_shopify_zombie_plan,
        COALESCE(
            LAG(inc_amount, 1, NULL) OVER (PARTITION BY shop_subdomain ORDER BY dt),
            0
        ) AS day_before_inc_amount
    FROM {{ ref("mesa_shop_days") }}
),

churn_dates AS (
    SELECT
        shop_subdomain,
        {# day_before_inc_amount, #}
        MAX(dt) AS churned_on_pt
    FROM shops
    LEFT JOIN inc_amount_days_and_day_befores USING (shop_subdomain)
    WHERE
        (uninstalled_at_pt::date = dt AND inc_amount > 0)
        OR
        (inc_amount_days_and_day_befores.is_shopify_zombie_plan AND day_before_inc_amount > 0)
        OR
        (inc_amount = 0 AND day_before_inc_amount > 0)
    GROUP BY 1, uninstalled_at_pt
),

final AS (
    SELECT
        *
        EXCLUDE (
            has_had_launch_session,
            avg_current_gmv_usd,
            avg_initial_gmv_usd,
            churned_on_pt,
            last_plan_price
        )
        REPLACE (
            (
                COALESCE((1.0 * shopify_shop_gmv_initial_total_usd) >= 3000, FALSE)
                OR
                shopify_plan_name IN ('professional', 'unlimited', 'shopify_plus')
            ) AS is_mql
        ),
        NOT activation_date_pt IS NULL AS is_activated,
        IFF(is_activated, 'activated', 'onboarding') AS funnel_phase,
        {{
            dbt.datediff(
                "first_installed_at_pt::DATE", "activation_date_pt", "days"
            )
        }} AS days_to_activation,
        COALESCE(
            has_had_launch_session, not launch_session_date is NULL
        ) AS has_had_launch_session,
        {{ dbt.datediff("launch_session_date", "activation_date_pt", "days") }}
            AS days_from_launch_session_to_activation,
        shopify_shop_gmv_current_total_usd / nullif(avg_current_gmv_usd, 0)
            - 1 AS shopify_shop_gmv_current_cohort_avg_percent,
        shopify_shop_gmv_initial_total_usd / nullif(avg_initial_gmv_usd, 0)
            - 1 AS shopify_shop_gmv_initial_cohort_avg_percent,
        CASE
            WHEN shopify_shop_gmv_current_total_usd < 100
            THEN 100
            WHEN shopify_shop_gmv_current_total_usd < 1000
            THEN 1000
            WHEN shopify_shop_gmv_current_total_usd < 10000
            THEN 10000
            WHEN shopify_shop_gmv_current_total_usd < 50000
            THEN 50000
            WHEN shopify_shop_gmv_current_total_usd < 100000
            THEN 100000
            WHEN shopify_shop_gmv_current_total_usd < 250000
            THEN 250000
            WHEN shopify_shop_gmv_current_total_usd < 500000
            THEN 500000
            WHEN shopify_shop_gmv_current_total_usd < 750000
            THEN 750000
            WHEN shopify_shop_gmv_current_total_usd < 1000000
            THEN 1000000
            WHEN shopify_shop_gmv_current_total_usd < 2000000
            THEN 2000000
            WHEN shopify_shop_gmv_current_total_usd < 5000000
            THEN 5000000
            WHEN shopify_shop_gmv_current_total_usd < 10000000
            THEN 10000000
            WHEN shopify_shop_gmv_current_total_usd < 20000000
            THEN 20000000
            WHEN shopify_shop_gmv_current_total_usd < 50000000
            THEN 50000000
            WHEN shopify_shop_gmv_current_total_usd < 100000000
            THEN 100000000
            WHEN shopify_shop_gmv_current_total_usd < 200000000
            THEN 200000000
            WHEN shopify_shop_gmv_current_total_usd < 500000000
            THEN 500000000
            WHEN shopify_shop_gmv_current_total_usd < 1000000000
            THEN 1000000000
        end AS shopify_shop_gmv_current_total_tier,

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
            WHEN store_leads_estimated_monthly_sales < 1000
            THEN 'A-Under $1,000'
            WHEN store_leads_estimated_monthly_sales < 5000
            THEN 'B-$1,000-$5,000'
            WHEN store_leads_estimated_monthly_sales < 10000
            THEN 'C-$5,000-$10,000'
            WHEN store_leads_estimated_monthly_sales < 25000
            THEN 'D-$10,000-$25,000'
            WHEN store_leads_estimated_monthly_sales < 50000
            THEN 'E-$25,000-$50,000'
            WHEN store_leads_estimated_monthly_sales < 100000
            THEN 'F-$50,000-$100,000'
            WHEN store_leads_estimated_monthly_sales < 250000
            THEN 'G-$100,000-$250,000'
            WHEN store_leads_estimated_monthly_sales < 500000
            THEN 'H-$250,000-$500,000'
            WHEN store_leads_estimated_monthly_sales < 1000000
            THEN 'I-$500,000-$1,000,000'
            WHEN store_leads_estimated_monthly_sales < 2500000
            THEN 'J-$1,000,000-$2,500,000'
            ELSE 'K-$2,500,000+'
        end AS store_leads_estimated_monthly_sales_bucket,
        COALESCE(trial_ends_pt >= current_date, FALSE) AS is_in_trial,

        yesterdays_inc_amount > 0
            AND NOT is_shopify_zombie_plan
            AND NOT is_in_trial
            AND billing_accounts.plan_name not ilike '%free%'
            AND install_status = 'active' AS is_currently_paying,
        average_daily_revenue = 0
            AND NOT is_shopify_zombie_plan
            AND NOT is_in_trial
            AND billing_accounts.plan_name not ilike '%free%'
            AND install_status = 'active' AS is_likely_shopify_plus_dev_store,

        CASE
            WHEN has_ever_upgraded_to_paid_plan
                THEN install_status = 'uninstalled' OR NOT is_currently_paying
        END AS has_churned_paid,

        has_ever_upgraded_to_paid_plan AND plan_change_chain ilike '%$0' AS did_pay_and_then_downgrade_to_free,

        CASE has_done_a_trial
            WHEN install_status = 'uninstalled' or not is_in_trial
                THEN NOT has_ever_upgraded_to_paid_plan
        END AS has_churned_during_trial,

        case
            when not has_done_a_trial
                then '1-Has Not Done A Trial'
            when
                has_churned_during_trial
                THEN '3-Churned During Trial'
            WHEN is_in_trial AND NOT is_currently_paying
                THEN '2-Currently In Trial'
            WHEN has_churned_paid or did_pay_and_then_downgrade_to_free
                THEN '4-Paid and Then Churned'
            WHEN is_currently_paying
                THEN '5-Currently Paying'
            ELSE '6-Not trial but a paid plan (should not happen)'
        END AS plan_upgrade_funnel_status,

        CASE
            WHEN max_workflow_steps <= 2
                THEN 1
            WHEN max_workflow_steps between 3 AND 4
                THEN 2
            ELSE 3
        END AS virtual_plan_step_qualifier,
        IFF(is_using_pro_apps, 2, 1) AS virtual_plan_pro_app_qualifier,
        CASE
            WHEN workflow_run_attempt_rolling_thirty_day_count <= 500
            THEN 1
            WHEN workflow_run_attempt_rolling_thirty_day_count BETWEEN 501 AND 5000
            THEN 2
            WHEN
                workflow_run_attempt_rolling_thirty_day_count BETWEEN 5001 AND 10000
            THEN 3
            ELSE 4
        end AS virtual_plan_workflow_run_attempt_qualifier,
        GREATEST(
            virtual_plan_step_qualifier,
            virtual_plan_pro_app_qualifier,
            virtual_plan_workflow_run_attempt_qualifier
        ) AS virtual_plan,
        COALESCE(
            LEAST(
                COALESCE(first_newsletter_sent_at_pt, CURRENT_TIMESTAMP()),
                COALESCE(first_broadcast_email_clicked_at_pt, CURRENT_TIMESTAMP()),
                COALESCE(first_broadcast_email_open_at_pt, CURRENT_TIMESTAMP()),
                COALESCE(
                    first_broadcast_email_converted_at_pt, CURRENT_TIMESTAMP()
                ),
                COALESCE(first_journey_sent_at_pt, CURRENT_TIMESTAMP()),
                COALESCE(first_journey_email_open_at_pt, CURRENT_TIMESTAMP()),
                COALESCE(first_journey_email_converted_at_pt, CURRENT_TIMESTAMP())
            )
            < first_installed_at_pt,
            FALSE
        ) AS is_email_acquisition,
        iff(
            has_ever_upgraded_to_paid_plan AND NOT is_currently_paying,
            churned_on_pt,
            NULL
        ) AS churned_on_pt,
        floor(
            datediff('day', first_plan_upgrade_date, churned_on_pt)
        ) AS churned_customer_duration_in_days,
        floor(
            datediff('days', first_plan_upgrade_date, churned_on_pt) / 7
        ) AS churned_customer_duration_in_weeks,
        floor(
            datediff('days', first_plan_upgrade_date, churned_on_pt) / 30
        ) AS churned_customer_duration_in_months,
        COALESCE(
            iff(projected_mrr > 0, projected_mrr, iff(last_plan_price > 0, last_plan_price, plan_price)), 0
        ) AS shop_value_per_month

    FROM shops
    LEFT JOIN billing_accounts USING (shop_subdomain)
    LEFT JOIN price_per_actions USING (shop_subdomain)
    LEFT JOIN csm_support USING (shop_subdomain)
    LEFT JOIN workflow_counts USING (shop_subdomain)
    LEFT JOIN workflow_run_counts USING (shop_subdomain)
    LEFT JOIN successful_workflow_run_counts USING (shop_subdomain)
    LEFT JOIN app_pageview_bookend_times USING (shop_subdomain)
    LEFT JOIN current_rolling_counts USING (shop_subdomain)
    LEFT JOIN simple_shop_attribution USING (shop_subdomain)
    LEFT JOIN max_funnel_steps USING (shop_subdomain)
    LEFT JOIN total_ltv_revenue USING (shop_subdomain)
    LEFT JOIN shop_infos USING (shop_subdomain)
    LEFT JOIN cohort_average_current_shop_gmv
    LEFT JOIN cohort_average_initial_shop_gmv USING (cohort_month)
    LEFT JOIN email_open_details USING (shop_subdomain)
    LEFT JOIN email_click_details USING (shop_subdomain)
    LEFT JOIN email_conversion_details USING (shop_subdomain)
    LEFT JOIN thirty_day_revenue USING (shop_subdomain)
    LEFT JOIN first_workflow_keys USING (shop_subdomain)
    LEFT JOIN max_workflow_steps USING (shop_subdomain)
    LEFT JOIN int_shop_integration_app_rows USING (shop_subdomain)
    LEFT JOIN plan_change_chains USING (shop_subdomain)
    LEFT JOIN email_unsubscribe_details USING (shop_subdomain)
    LEFT JOIN first_newsletter_deliveries USING (shop_subdomain)
    LEFT JOIN first_journey_deliveries USING (shop_subdomain)
    LEFT JOIN churn_dates USING (shop_subdomain)
    LEFT JOIN workflow_source_destination_pairs USING (shop_subdomain)
    LEFT JOIN last_plan_prices USING (shop_subdomain)
)

SELECT *
FROM final
