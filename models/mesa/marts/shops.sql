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

shop_freezes AS (
    SELECT *
    FROM {{ ref('int_frozen_store_events') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY shop_subdomain ORDER BY occurred_at DESC
    ) = 1
),

cancelled_charge_events AS (
    SELECT *
    FROM {{ ref('int_charge_cancelled_events') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY shop_subdomain ORDER BY occurred_at DESC
    ) = 1
),

shop_deactivated_events AS (
    SELECT *
    FROM {{ ref('int_shop_deactivated_events') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY shop_subdomain ORDER BY occurred_at DESC
    ) = 1
),

churn_types AS (
    SELECT
        shop_subdomain,
        CASE
            WHEN has_ever_upgraded_to_paid_plan
                THEN
                    CASE
                        WHEN shop_deactivated_events.occurred_at IS NOT NULL
                            THEN 'shop-deactivated'
                        WHEN cancelled_charge_events.occurred_at IS NOT NULL
                            THEN IFF(is_cancelled_from_uninstall, 'uninstalled', 'downgraded')
                        WHEN shop_freezes.occurred_at IS NOT NULL
                            THEN 'shop-frozen'
                        WHEN install_status = 'uninstalled'
                            THEN 'uninstalled'
                    END
        END AS churn_type
    FROM shops
    LEFT JOIN shop_freezes USING (shop_subdomain)
    LEFT JOIN cancelled_charge_events USING (shop_subdomain)
    LEFT JOIN shop_deactivated_events USING (shop_subdomain)
),

paid_days_tiers AS (
    SELECT
        shop_subdomain,
        CASE
            WHEN paid_days_completed = 0 THEN NULL
            WHEN paid_days_completed = 1 THEN 'A-1 day'
            WHEN paid_days_completed BETWEEN 2 AND 7 THEN 'B-2-7 days'
            WHEN paid_days_completed BETWEEN 8 AND 30 THEN 'C-8-30 days'
            WHEN paid_days_completed BETWEEN 31 AND 60 THEN 'D-31-60 days'
            WHEN paid_days_completed BETWEEN 61 AND 90 THEN 'E-61-90 days'
            WHEN paid_days_completed BETWEEN 91 AND 180 THEN 'F-91-180 days'
            WHEN paid_days_completed BETWEEN 181 AND 365 THEN 'G-181-365 days'
            WHEN paid_days_completed > 365 THEN 'H-365+ days'
        END AS paid_days_completed_tier
    FROM shops
),

final AS (
    SELECT
        shops.*,
        billing_accounts.plan_name,
        price_per_action,
        has_csm_support,
        workflows_current_count,
        workflows_enabled_current_count,
        templates_installed_count,
        is_using_pro_apps,
        has_puc_workflow,
        integration_apps_enabled_count,
        pro_apps_enabled_count,
        simple_shop_attribution.* EXCLUDE (shop_subdomain),
        max_funnel_step_achieved_at_pt,
        max_funnel_step,
        max_funnel_step_name,
        max_funnel_steps.has_a_workflow,
        has_saved_a_workflow,
        has_enabled_a_workflow,
        shop_infos.* EXCLUDE (shop_subdomain),
        first_workflow_keys.* EXCLUDE (shop_subdomain),
        max_workflow_steps,
        max_workflow_steps_with_deleted,
        source_destination_pairs_list,
        plan_change_count,
        plan_change_chain,
        last_plan_price,
        churn_type,
        paid_days_completed_tier,
        NOT activation_date_pt IS NULL AS is_activated,
        IFF(is_activated, 'activated', 'onboarding') AS funnel_phase,
        {{
            dbt.datediff(
                "first_installed_at_pt::DATE", "activation_date_pt", "days"
            )
        }} AS days_to_activation,
        COALESCE(
            has_had_launch_session, not launch_session_date is NULL
        ) AS has_had_launch_session_final,
        {{ dbt.datediff("launch_session_date", "activation_date_pt", "days") }}
            AS days_from_launch_session_to_activation,
        shopify_shop_gmv_current_total_usd / nullif(avg_current_gmv_usd, 0)
            - 1 AS shopify_shop_gmv_current_cohort_avg_percent,
        shopify_shop_gmv_initial_total_usd / nullif(avg_initial_gmv_usd, 0)
            - 1 AS shopify_shop_gmv_initial_cohort_avg_percent,
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
        end AS store_leads_estimated_monthly_sales_bucket,
        COALESCE(trial_ends_pt >= current_date, FALSE) AS is_in_trial,
        CASE
            WHEN max_workflow_steps <= 2 THEN 1
            WHEN max_workflow_steps between 3 AND 4 THEN 2
            ELSE 3
        END AS virtual_plan_step_qualifier,
        IFF(is_using_pro_apps, 2, 1) AS virtual_plan_pro_app_qualifier,
        GREATEST(
            virtual_plan_step_qualifier,
            virtual_plan_pro_app_qualifier
        ) AS virtual_plan
    FROM shops
    LEFT JOIN billing_accounts USING (shop_subdomain)
    LEFT JOIN price_per_actions USING (shop_subdomain)
    LEFT JOIN csm_support USING (shop_subdomain)
    LEFT JOIN workflow_counts USING (shop_subdomain)
    LEFT JOIN simple_shop_attribution USING (shop_subdomain)
    LEFT JOIN max_funnel_steps USING (shop_subdomain)
    LEFT JOIN shop_infos USING (shop_subdomain)
    LEFT JOIN cohort_average_current_shop_gmv
    LEFT JOIN cohort_average_initial_shop_gmv USING (cohort_month)
    LEFT JOIN first_workflow_keys USING (shop_subdomain)
    LEFT JOIN max_workflow_steps USING (shop_subdomain)
    LEFT JOIN int_shop_integration_app_rows USING (shop_subdomain)
    LEFT JOIN plan_change_chains USING (shop_subdomain)
    LEFT JOIN churn_types USING (shop_subdomain)
    LEFT JOIN workflow_source_destination_pairs USING (shop_subdomain)
    LEFT JOIN last_plan_prices USING (shop_subdomain)
    LEFT JOIN paid_days_tiers USING (shop_subdomain)
)

SELECT *
FROM final
