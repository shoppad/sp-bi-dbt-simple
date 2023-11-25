with
    shops as (select * from {{ ref("int_shops") }}),

    billing_accounts as (select * from {{ ref("stg_mesa_billing_accounts") }}),

    price_per_actions as (
        select shop_subdomain, "value" as price_per_action
        from {{ ref("stg_shop_entitlements") }}
        where "name" = 'price_per_action'
    ),

    csm_support as (
        select shop_subdomain, coalesce("value" = 'csm', false) as has_csm_support
        from shops
        left join
            (
                select *
                from {{ ref("stg_shop_entitlements") }}
                where "name" = 'support'
            ) using (shop_subdomain)

    ),

    workflows as (select * from {{ ref("workflows") }} where is_deleted = false),

    workflow_counts as (
        select
            shop_subdomain,
            count_if(workflows.step_count > 1) as workflows_current_count,
            count_if(
                workflows.step_count > 1 and workflows.is_enabled
            ) as workflows_enabled_current_count,
            count(distinct workflows.template_name) as templates_installed_count,
            count_if(workflows.has_pro_app and workflows.is_enabled)
            > 0 as is_using_pro_apps,
            coalesce(
                sum(workflows.test_attempt_count) > 0, false
            ) as has_attempted_a_test,
            coalesce(
                sum(workflows.test_success_count) > 0, false
            ) as has_successfully_run_a_test,
            coalesce(max(workflows.is_puc), false) as has_puc_workflow
        from shops
        left join workflows using (shop_subdomain)
        group by 1
    ),

    int_shop_integration_app_rows as (
        select
            shop_subdomain,
            coalesce(
                count(distinct integration_app), 0
            ) as integration_apps_enabled_count,
            coalesce(count_if(is_pro_app), 0) as pro_apps_enabled_count
        from shops
        left join {{ ref("int_shop_integration_app_rows") }} using (shop_subdomain)
        group by 1
    ),

    workflow_run_counts as (
        select
            shop_subdomain,
            coalesce(
                count(distinct workflow_id), 0
            ) as unique_workflows_attempted_count,
            coalesce(
                count(workflow_runs.workflow_run_id), 0
            ) as workflow_runs_attempted_count,
            coalesce(count_if(workflow_runs.is_stop), 0) as workflow_runs_stop_count,
            coalesce(count_if(workflow_runs.is_failure), 0) as workflow_runs_fail_count
        from shops
        left join {{ ref("workflow_runs") }} using (shop_subdomain)
        group by 1
    ),

    successful_workflow_run_counts as (
        select
            shops.shop_subdomain,
            coalesce(count(workflow_run_id), 0) as workflow_run_success_count,
            coalesce(
                count(distinct workflow_id), 0
            ) as unique_workflows_successfully_run_count
        from shops
        left join
            {{ ref("workflow_runs") }}
            on shops.shop_subdomain = workflow_runs.shop_subdomain
            and workflow_runs.run_status = 'success'
        group by 1
    ),

    app_pageview_bookend_times as (
        select
            user_id as shop_subdomain,
            {{ pacific_timestamp("MIN(tstamp)") }} as first_seen_in_app_at_pt,
            {{ pacific_timestamp("MAX(tstamp)") }} as last_seen_in_app_at_pt,
            sum(duration_in_s) / 60 as minutes_using_app
        from {{ ref("segment_web_page_views__sessionized") }}
        left join {{ ref("segment_web_sessions") }} using (session_id)
        where page_url_host = 'app.getmesa.com'
        group by 1
    ),

    yesterdays as (
        select *
        from {{ ref("mesa_shop_days") }}
        where dt = {{ pacific_timestamp("CURRENT_DATE()") }}::date - interval '1 day'
    ),

    current_rolling_counts as (
        select
            shop_subdomain,
            coalesce(
                workflow_run_attempt_rolling_thirty_day_count, 0
            ) as workflow_run_attempt_rolling_thirty_day_count,
            coalesce(
                workflow_run_success_rolling_thirty_day_count, 0
            ) as workflow_run_success_rolling_thirty_day_count,
            coalesce(
                workflow_run_failure_rolling_thirty_day_count, 0
            ) as workflow_run_failure_rolling_thirty_day_count,
            coalesce(
                workflow_run_stop_rolling_thirty_day_count, 0
            ) as workflow_run_stop_rolling_thirty_day_count,
            coalesce(
                workflow_run_attempt_rolling_year_count, 0
            ) as workflow_run_attempt_rolling_year_count,
            coalesce(
                workflow_run_success_rolling_year_count, 0
            ) as workflow_run_success_rolling_year_count,
            coalesce(
                workflow_run_failure_rolling_year_count, 0
            ) as workflow_run_failure_rolling_year_count,
            coalesce(
                workflow_run_stop_rolling_year_count, 0
            ) as workflow_run_stop_rolling_year_count,
            coalesce(
                income_rolling_thirty_day_total, 0
            ) as income_rolling_thirty_day_total,
            coalesce(income_rolling_year_total, 0) as income_rolling_year_total,
            coalesce(
                total_workflow_steps_rolling_thirty_day_count, 0
            ) as total_workflow_steps_rolling_thirty_day_count,
            coalesce(
                input_step_rolling_thirty_day_count, 0
            ) as input_step_rolling_thirty_day_count,
            coalesce(
                output_step_rolling_thirty_day_count, 0
            ) as output_step_rolling_thirty_day_count,
            coalesce(inc_amount, 0) as yesterdays_inc_amount
        from shops
        left join yesterdays using (shop_subdomain)
    ),

    install_sources as (select * from {{ ref("int_shop_install_sources") }}),

    max_funnel_steps as (
        select
            shop_subdomain,
            achieved_at_pt as max_funnel_step_achieved_at_pt,
            step_order as max_funnel_step,
            case
                when activation_date_pt is not null
                then '7-Activated'
                else (step_order || '-' || name)
            end as max_funnel_step_name,
            coalesce(step_order, 0) >= 3 as has_a_workflow,
            coalesce(step_order, 0) >= 4 as has_saved_a_workflow,
            coalesce(step_order, 0) >= 6 as has_enabled_a_workflow
        from shops
        left join {{ ref("int_mesa_shop_funnel_achievements") }} using (shop_subdomain)
        qualify
            row_number() over (partition by shop_subdomain order by step_order desc) = 1
    ),

    total_ltv_revenue as (
        select shop_subdomain, coalesce(sum(inc_amount), 0) as total_ltv_revenue
        from shops
        left join {{ ref("mesa_shop_days") }} using (shop_subdomain)
        group by 1
    ),

    shop_infos as (
        select
            * exclude (
                updated_at,
                shopify_createdat,
                analytics_gmv,
                shopify_plandisplayname,
                shopify_inactiveat,
                analytics_orders,
                shopify_planname
            )
        from {{ ref("int_shop_infos") }}
    ),

    cohort_average_current_shop_gmv as (
        select avg(shopify_shop_gmv_current_total_usd) as avg_current_gmv_usd
        from {{ ref("int_shops") }}
    ),

    cohort_average_initial_shop_gmv as (
        select
            cohort_month, avg(shopify_shop_gmv_initial_total_usd) as avg_initial_gmv_usd
        from {{ ref("int_shops") }}
        group by 1
    ),

    last_thirty_days as (
        select *
        from {{ ref("mesa_shop_days") }}
        where dt >= current_date - interval '30 day' and inc_amount > 0
    ),

    thirty_day_revenue as (
        select
            shop_subdomain,
            coalesce(avg(daily_usage_revenue), 0) as average_daily_usage_revenue,
            coalesce(avg(inc_amount), 0) as average_daily_revenue,
            average_daily_revenue * 30 as projected_mrr,
            coalesce(sum(inc_amount), 0) as total_thirty_day_revenue
        from shops
        left join last_thirty_days using (shop_subdomain)
        group by 1
    ),

    email_open_details as (
        select
            shop_subdomain,
            min(
                case when email_type = 'broadcast' then opened_at_pt else null end
            ) as first_broadcast_email_open_at_pt,
            max(
                case when email_type = 'broadcast' then opened_at_pt else null end
            ) as last_broadcast_email_open_at_pt,
            coalesce(
                count(
                    distinct case
                        when email_type = 'broadcast' then email_id else null
                    end
                ),
                0
            ) as broadcast_email_opens_count,
            broadcast_email_opens_count > 0 as has_opened_broadcast_email,

            min(
                case when email_type = 'journey' then opened_at_pt else null end
            ) as first_journey_email_open_at_pt,
            max(
                case when email_type = 'journey' then opened_at_pt else null end
            ) as last_journey_email_open_at_pt,
            coalesce(
                count(
                    distinct case
                        when email_type = 'journey' then email_id else null
                    end
                ),
                0
            ) as journey_email_opens_count,
            journey_email_opens_count > 0 as has_opened_journey_email
        from shops
        left join {{ ref("stg_email_opens") }} using (shop_subdomain)
        group by 1
    ),

    email_click_details as (
        select
            shop_subdomain,
            min(
                case when email_type = 'broadcast' then clicked_at_pt else null end
            ) as first_broadcast_email_clicked_at_pt,
            max(
                case when email_type = 'broadcast' then clicked_at_pt else null end
            ) as last_broadcast_email_clicked_at_pt,
            coalesce(
                count(
                    distinct case
                        when email_type = 'broadcast' then email_id else null
                    end
                ),
                0
            ) as broadcast_email_click_count,
            broadcast_email_click_count > 0 as has_clicked_broadcast_email,

            min(
                case when email_type = 'journey' then clicked_at_pt else null end
            ) as first_journey_email_clicked_at_pt,
            max(
                case when email_type = 'journey' then clicked_at_pt else null end
            ) as last_journey_email_clicked_at_pt,
            coalesce(
                count(
                    distinct case
                        when email_type = 'journey' then email_id else null
                    end
                ),
                0
            ) as journey_email_click_count,
            journey_email_click_count > 0 as has_clicked_journey_email
        from shops
        left join {{ ref("stg_email_clicks") }} using (shop_subdomain)
        group by 1
    ),

    email_conversion_details as (
        select
            shop_subdomain,
            min(
                case when email_type = 'broadcast' then converted_at_pt else null end
            ) as first_broadcast_email_converted_at_pt,
            max(
                case when email_type = 'broadcast' then converted_at_pt else null end
            ) as last_broadcast_email_converted_at_pt,
            coalesce(
                count(
                    distinct case
                        when email_type = 'broadcast' then email_id else null
                    end
                ),
                0
            ) as broadcast_email_conversion_count,
            broadcast_email_conversion_count > 0 as has_converted_via_broadcast_email,

            min(
                case when email_type = 'journey' then converted_at_pt else null end
            ) as first_journey_email_converted_at_pt,
            max(
                case when email_type = 'journey' then converted_at_pt else null end
            ) as last_journey_email_converted_at_pt,
            coalesce(
                count(
                    distinct case
                        when email_type = 'journey' then email_id else null
                    end
                ),
                0
            ) as journey_email_conversion_count,
            journey_email_conversion_count > 0 as has_converted_via_journey_email
        from shops
        left join {{ ref("stg_email_conversions") }} using (shop_subdomain)
        group by 1
    ),

    email_unsubscribe_details as (
        select
            shop_subdomain,
            coalesce(
                email_unsubscribe_email_type is not null, false
            ) as has_unsubscribed_from_email,
            email_unsubscribe_email_type,
            email_unsubscribe_email_name
        from shops
        left join {{ ref("stg_email_unsubscribes") }} using (shop_subdomain)
        qualify
            row_number() over (
                partition by shop_subdomain order by __hevo__ingested_at desc
            )
            = 1
    ),

    first_workflow_keys as (select * from {{ ref("int_first_workflow_keys") }}),

    max_workflow_steps as (
        select
            shop_subdomain,
            coalesce(max(step_count), 0) as max_workflow_steps,
            coalesce(
                max(step_count_with_deleted), 0
            ) as max_workflow_steps_with_deleted,
            coalesce(max(step_count) >= 2, false) as has_a_workflow
        from shops
        left join {{ ref("workflows") }} using (shop_subdomain)
        group by 1
    ),

    workflow_source_destination_pairs as (
        select
            listagg(distinct source_destination_pair, ',') within group (
                order by source_destination_pair asc
            ) as source_destination_pairs_list,
            shop_subdomain
        from workflows
        group by 2
    ),

    plan_change_chains as (
        select
            shop_subdomain,
            count(distinct plan) as plan_change_count,
            listagg(
                concat(
                    iff(previous_price is null or previous_price <= price, '↑:', '↓:'),
                    planid,
                    ':$',
                    price
                ),
                ' • '
            ) within group (order by changed_at_pt asc) as plan_change_chain
        from shops
        left join {{ ref("stg_mesa_plan_changes") }} using (shop_subdomain)
        group by 1
    ),

    first_newsletter_deliveries as (
        select * from {{ ref("int_first_newsletter_deliveries") }}
    ),

    first_journey_deliveries as (
        select * from {{ ref("int_first_journey_deliveries") }}
    ),

    inc_amount_days_and_day_befores as (
        select
            shop_subdomain,
            dt,
            inc_amount,
            coalesce(
                lag(inc_amount, 1, null) over (partition by shop_subdomain order by dt),
                0
            ) as day_before_inc_amount
        from {{ ref("mesa_shop_days") }}
    ),

    churn_dates as (
        select
            shop_subdomain,
            {# day_before_inc_amount, #}
            max(dt) as churned_on_pt
        from inc_amount_days_and_day_befores
        where inc_amount = 0 and day_before_inc_amount > 0
        group by 1
        qualify
            row_number() over (partition by shop_subdomain order by churned_on_pt desc)
            = 1
    ),

    final as (
        select
            *
            exclude (
                has_had_launch_session,
                avg_current_gmv_usd,
                avg_initial_gmv_usd,
                churned_on_pt
            )
            replace (
                (
                    coalesce((1.0 * shopify_shop_gmv_initial_total_usd) > 3000, false)
                    or
                    SHOPIFY_PLAN_NAME in ('professional', 'unlimited', 'shopify_plus')
                ) as is_mql
            ),
            not (activation_date_pt is null) as is_activated,
            iff(is_activated, 'activated', 'onboarding') as funnel_phase,
            {{
                dbt.datediff(
                    "first_installed_at_pt::DATE", "activation_date_pt", "days"
                )
            }} as days_to_activation,
            coalesce(
                has_had_launch_session, not launch_session_date is null
            ) as has_had_launch_session,
            {{ dbt.datediff("launch_session_date", "activation_date_pt", "days") }}
            as days_from_launch_session_to_activation,
            shopify_shop_gmv_current_total_usd / nullif(avg_current_gmv_usd, 0)
            - 1 as shopify_shop_gmv_current_cohort_avg_percent,
            shopify_shop_gmv_initial_total_usd / nullif(avg_initial_gmv_usd, 0)
            - 1 as shopify_shop_gmv_initial_cohort_avg_percent,
            case
                when shopify_shop_gmv_current_total_usd < 100
                then 100
                when shopify_shop_gmv_current_total_usd < 1000
                then 1000
                when shopify_shop_gmv_current_total_usd < 10000
                then 10000
                when shopify_shop_gmv_current_total_usd < 50000
                then 50000
                when shopify_shop_gmv_current_total_usd < 100000
                then 100000
                when shopify_shop_gmv_current_total_usd < 250000
                then 250000
                when shopify_shop_gmv_current_total_usd < 500000
                then 500000
                when shopify_shop_gmv_current_total_usd < 750000
                then 750000
                when shopify_shop_gmv_current_total_usd < 1000000
                then 1000000
                when shopify_shop_gmv_current_total_usd < 2000000
                then 2000000
                when shopify_shop_gmv_current_total_usd < 5000000
                then 5000000
                when shopify_shop_gmv_current_total_usd < 10000000
                then 10000000
                when shopify_shop_gmv_current_total_usd < 20000000
                then 20000000
                when shopify_shop_gmv_current_total_usd < 50000000
                then 50000000
                when shopify_shop_gmv_current_total_usd < 100000000
                then 100000000
                when shopify_shop_gmv_current_total_usd < 200000000
                then 200000000
                when shopify_shop_gmv_current_total_usd < 500000000
                then 500000000
                when shopify_shop_gmv_current_total_usd < 1000000000
                then 1000000000
            end as shopify_shop_gmv_current_total_tier,

            'https://www.theshoppad.com/homeroom.theshoppad.com/admin/backdoor/'
            || shop_subdomain
            || '/mesa' as backdoor_url,
            'https://insights.hotjar.com/sites/1547357/'
            || 'workspaces/1288874/playbacks/list?'
            || 'filters=%7B%22AND%22:%5B%7B%22DAYS_AGO%22:%7B%22created%22:365%7D%7D,'
            || '%7B%22EQUAL%22:%7B%22user_attributes.str.user_id%22:%22'
            || shop_subdomain
            || '%22%7D%7D%5D%7D' as hotjar_url,
            case
                when store_leads_estimated_monthly_sales < 1000
                then 'A-Under $1,000'
                when store_leads_estimated_monthly_sales < 5000
                then 'B-$1,000-$5,000'
                when store_leads_estimated_monthly_sales < 10000
                then 'C-$5,000-$10,000'
                when store_leads_estimated_monthly_sales < 25000
                then 'D-$10,000-$25,000'
                when store_leads_estimated_monthly_sales < 50000
                then 'E-$25,000-$50,000'
                when store_leads_estimated_monthly_sales < 100000
                then 'F-$50,000-$100,000'
                when store_leads_estimated_monthly_sales < 250000
                then 'G-$100,000-$250,000'
                when store_leads_estimated_monthly_sales < 500000
                then 'H-$250,000-$500,000'
                when store_leads_estimated_monthly_sales < 1000000
                then 'I-$500,000-$1,000,000'
                when store_leads_estimated_monthly_sales < 2500000
                then 'J-$1,000,000-$2,500,000'
                else 'K-$2,500,000+'
            end as store_leads_estimated_monthly_sales_bucket,
            coalesce(trial_ends_pt >= current_date, false) as is_in_trial,
            yesterdays_inc_amount > 0
            and not is_shopify_zombie_plan
            and not is_in_trial
            and billing_accounts.plan_name not ilike '%free%'
            and install_status = 'active' as is_currently_paying,
            average_daily_revenue = 0
            and not is_shopify_zombie_plan
            and not is_in_trial
            and billing_accounts.plan_name not ilike '%free%'
            and install_status = 'active' as is_likely_shopify_plus_dev_store,
            plan_change_chain ilike '%$0' as did_pay_and_then_downgrade_to_free,
            has_ever_upgraded_to_paid_plan and not is_currently_paying as has_churned,
            case
                when not has_done_a_trial
                then '1-Has Not Done A Trial'
                when
                    (install_status = 'uninstalled' or not is_in_trial)
                    and has_done_a_trial
                    and not has_ever_upgraded_to_paid_plan
                then '3-Churned During Trial'
                when is_in_trial and not is_currently_paying
                then '2-Currently In Trial'
                when has_churned or did_pay_and_then_downgrade_to_free
                then '4-Paid and Then Churned'
                when is_currently_paying
                then '5-Currently Paying'
                else '6-Not trial but a paid plan (should not happen)'
            end as plan_upgrade_funnel_status,

            case
                when max_workflow_steps <= 2
                then 1
                when max_workflow_steps between 3 and 4
                then 2
                else 3
            end as virtual_plan_step_qualifier,
            iff(is_using_pro_apps, 2, 1) as virtual_plan_pro_app_qualifier,
            case
                when workflow_run_attempt_rolling_thirty_day_count <= 500
                then 1
                when workflow_run_attempt_rolling_thirty_day_count between 501 and 5000
                then 2
                when
                    workflow_run_attempt_rolling_thirty_day_count between 5001 and 10000
                then 3
                else 4
            end as virtual_plan_workflow_run_attempt_qualifier,
            greatest(
                virtual_plan_step_qualifier,
                virtual_plan_pro_app_qualifier,
                virtual_plan_workflow_run_attempt_qualifier
            ) as virtual_plan,
            coalesce(
                least(
                    coalesce(first_newsletter_sent_at_pt, current_timestamp()),
                    coalesce(first_broadcast_email_clicked_at_pt, current_timestamp()),
                    coalesce(first_broadcast_email_open_at_pt, current_timestamp()),
                    coalesce(
                        first_broadcast_email_converted_at_pt, current_timestamp()
                    ),
                    coalesce(first_journey_sent_at_pt, current_timestamp()),
                    coalesce(first_journey_email_open_at_pt, current_timestamp()),
                    coalesce(first_journey_email_converted_at_pt, current_timestamp())
                )
                < first_installed_at_pt,
                false
            ) as is_email_acquisition,
            iff(
                has_ever_upgraded_to_paid_plan and not is_currently_paying,
                churned_on_pt,
                null
            ) as churned_on_pt,
            floor(
                datediff('day', first_plan_upgrade_date, churned_on_pt)
            ) as churned_customer_duration_in_days,
            floor(
                datediff('days', first_plan_upgrade_date, churned_on_pt) / 7
            ) as churned_customer_duration_in_weeks,
            floor(
                datediff('days', first_plan_upgrade_date, churned_on_pt) / 30
            ) as churned_customer_duration_in_months

        from shops
        left join billing_accounts using (shop_subdomain)
        left join price_per_actions using (shop_subdomain)
        left join csm_support using (shop_subdomain)
        left join workflow_counts using (shop_subdomain)
        left join workflow_run_counts using (shop_subdomain)
        left join successful_workflow_run_counts using (shop_subdomain)
        left join app_pageview_bookend_times using (shop_subdomain)
        left join current_rolling_counts using (shop_subdomain)
        left join install_sources using (shop_subdomain)
        left join max_funnel_steps using (shop_subdomain)
        left join total_ltv_revenue using (shop_subdomain)
        left join shop_infos using (shop_subdomain)
        left join cohort_average_current_shop_gmv
        left join cohort_average_initial_shop_gmv using (cohort_month)
        left join email_open_details using (shop_subdomain)
        left join email_click_details using (shop_subdomain)
        left join email_conversion_details using (shop_subdomain)
        left join thirty_day_revenue using (shop_subdomain)
        left join first_workflow_keys using (shop_subdomain)
        left join max_workflow_steps using (shop_subdomain)
        left join int_shop_integration_app_rows using (shop_subdomain)
        left join plan_change_chains using (shop_subdomain)
        left join email_unsubscribe_details using (shop_subdomain)
        left join first_newsletter_deliveries using (shop_subdomain)
        left join first_journey_deliveries using (shop_subdomain)
        left join churn_dates using (shop_subdomain)
        left join workflow_source_destination_pairs using (shop_subdomain)
    )

select *
from final
