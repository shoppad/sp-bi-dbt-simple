{% set source_table = ref("stg_shops") %}
{% set columns_to_skip = [
    "scopes",
    "billing",
    "status",
    "entitlements",
    "timestamp",
    "shopify",
    "usage",
    "config",
    "themes",
    "webhooks",
    "messages",
    "analytics",
    "schema",
    "handle",
    "method",
    "account",
    "wizard",
    "mongoid",
    "authtoken",
    "metabase",
] %}

WITH

{# ========== Base Shops (from int_shops) ========== #}

stg_shops AS (
    SELECT * FROM {{ source_table }}
),

activation_dates AS (
    SELECT uuid AS shop_subdomain, apps_mesa_meta_activatedat_value AS activation_date_pt
    FROM {{ source('php_segment', 'users') }}
),

launch_session_dates AS (
    SELECT
        shop_subdomain,
        IFF(
            meta_attribs.value:name = 'launchsessiondate',
            meta_attribs.value:value::date,
            NULL
        ) AS launch_session_date,
        NOT launch_session_date IS NULL AS has_had_launch_session
    FROM {{ ref("stg_shops") }}, LATERAL FLATTEN(input => meta) AS meta_attribs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY launch_session_date DESC) = 1
),

conversion_rates AS (
    SELECT currency, in_usd FROM {{ ref("currency_conversion_rates") }}
),

shops AS (
    SELECT
        {{
            groomed_column_list(source_table, except=columns_to_skip) | join(
                ",\n        "
            )
        }},
        shopify:currency::string AS currency,
        {{ pacific_timestamp("cast(shopify:created_at AS TIMESTAMP_LTZ)") }} AS shopify_shop_created_at_pt,
        shopify:country::string AS shopify_shop_country,
        status AS install_status,
        analytics:initial:orders_count::numeric AS shopify_shop_orders_initial_count,
        analytics:initial:orders_gmv::numeric AS shopify_shop_gmv_initial_total,
        analytics:orders:count::numeric AS shopify_shop_orders_current_count,
        analytics:orders:gmv::numeric AS shopify_shop_gmv_current_total,
        analytics:initial:shopify_plan_name::string AS initial_shopify_plan_name,
        COALESCE(wizard:builder:step = 'complete', FALSE) AS is_builder_wizard_completed,
        {{ datediff("shopify_shop_created_at_pt", "first_installed_at_pt", "day") }} AS age_of_store_at_install_in_days,
        {{ datediff("shopify_shop_created_at_pt", "first_installed_at_pt", "week") }} AS age_of_store_at_install_in_weeks,
        CASE
            WHEN age_of_store_at_install_in_days = 0 THEN '1-First Day'
            WHEN age_of_store_at_install_in_days <= 7 THEN '2-First Week (Day 2-7)'
            WHEN age_of_store_at_install_in_days <= 31 THEN '3-First Month (After First Week)'
            WHEN age_of_store_at_install_in_days <= 90 THEN '4-First Quarter (After First Month)'
            WHEN age_of_store_at_install_in_days <= 180 THEN '5-First Half (After First Quarter)'
            WHEN age_of_store_at_install_in_days <= 365 THEN '6-First Year (After First Half)'
            WHEN age_of_store_at_install_in_days <= 547 THEN '7-First 18 Months (After First Year)'
            WHEN age_of_store_at_install_in_days <= 730 THEN '8-First 2 Years (After 18 Months)'
            ELSE '9-2nd Year+'
        END AS age_of_store_at_install_bucket,
        activation_date_pt,
        launch_session_date,
        has_had_launch_session,
        1.0 * shopify_shop_gmv_initial_total * in_usd AS shopify_shop_gmv_initial_total_usd,
        1.0 * shopify_shop_gmv_current_total * in_usd AS shopify_shop_gmv_current_total_usd,
        COALESCE(in_usd IS NULL, FALSE) AS currency_not_supported
    FROM stg_shops
    LEFT JOIN activation_dates USING (shop_subdomain)
    LEFT JOIN launch_session_dates USING (shop_subdomain)
    LEFT JOIN conversion_rates USING (currency)
),

{# ========== Billing ========== #}

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

{# ========== Workflows (from int_workflows inlined) ========== #}

stg_workflows AS (
    SELECT * FROM {{ ref('stg_workflows') }}
),

workflow_steps AS (
    SELECT * FROM {{ ref('stg_workflow_steps') }}
    WHERE NOT is_deleted
),

deleted_workflow_steps AS (
    SELECT * FROM {{ ref('stg_workflow_steps') }}
    WHERE is_deleted
),

workflow_app_chains AS (
    SELECT
        workflow_id,
        LISTAGG(integration_app, ' • ') WITHIN GROUP (ORDER BY step_type ASC, position_in_workflow ASC) AS app_chain,
        LISTAGG(step_name, ' • ') WITHIN GROUP (ORDER BY step_type ASC, position_in_workflow ASC) AS step_chain
    FROM workflow_steps
    GROUP BY 1
),

workflow_counts AS (
    SELECT
        workflow_id,
        COUNT_IF(workflow_steps.is_pro_app) > 0 AS has_pro_app,
        COUNT(DISTINCT workflow_steps.workflow_step_id) AS step_count,
        COUNT(DISTINCT deleted_workflow_steps.workflow_step_id) AS deleted_step_count,
        COALESCE(step_count, 0) + COALESCE(deleted_step_count, 0) AS step_count_with_deleted
    FROM stg_workflows
    LEFT JOIN workflow_steps USING (workflow_id)
    LEFT JOIN deleted_workflow_steps USING (workflow_id)
    GROUP BY 1
),

workflows AS (
    SELECT
        stg_workflows.*,
        workflow_counts.has_pro_app,
        workflow_counts.step_count,
        workflow_counts.step_count_with_deleted,
        workflow_app_chains.app_chain,
        workflow_app_chains.step_chain,
        COALESCE(app_chain ILIKE ANY ('%googlesheets%', '%recharge%', '%infiniteoptions%', '%tracktor%', '%openai%', '%slack%'), FALSE) AS is_puc
    FROM stg_workflows
    LEFT JOIN workflow_counts USING (workflow_id)
    LEFT JOIN workflow_app_chains USING (workflow_id)
    WHERE is_deleted = FALSE
),

{# ========== Workflow Aggregations ========== #}

shop_workflow_counts AS (
    SELECT
        shop_subdomain,
        COUNT_IF(workflows.step_count > 1) AS workflows_current_count,
        COUNT_IF(workflows.step_count > 1 AND workflows.is_enabled) AS workflows_enabled_current_count,
        COUNT(DISTINCT workflows.template_name) AS templates_installed_count,
        COUNT_IF(workflows.has_pro_app AND workflows.is_enabled) > 0 AS is_using_pro_apps,
        COALESCE(MAX(workflows.is_puc), FALSE) AS has_puc_workflow
    FROM shops
    LEFT JOIN workflows USING (shop_subdomain)
    GROUP BY 1
),

{# ========== Integration Apps (from int_shop_integration_app_rows) ========== #}

enabled_workflows AS (
    SELECT workflow_id
    FROM {{ ref('stg_workflows') }}
    WHERE is_enabled AND NOT is_deleted
),

enabled_workflow_steps AS (
    SELECT shop_subdomain, integration_app, workflow_id
    FROM {{ ref('stg_workflow_steps') }}
    WHERE workflow_id IN (SELECT workflow_id FROM enabled_workflows)
),

shop_integration_apps AS (
    SELECT
        shop_subdomain,
        integration_app,
        integration_app IN ('{{ var("pro_apps") | join("', '") }}') AS is_pro_app
    FROM shops
    INNER JOIN enabled_workflow_steps USING (shop_subdomain)
    GROUP BY 1, 2
),

shop_integration_app_counts AS (
    SELECT
        shop_subdomain,
        COALESCE(COUNT(DISTINCT integration_app), 0) AS integration_apps_enabled_count,
        COALESCE(COUNT_IF(is_pro_app), 0) AS pro_apps_enabled_count
    FROM shops
    LEFT JOIN shop_integration_apps USING (shop_subdomain)
    GROUP BY 1
),

{# ========== Funnel Steps (from int_mesa_shop_funnel_achievements chain) ========== #}

funnel_steps AS (SELECT * FROM {{ ref("mesa_funnel_steps") }}),

mesa_flow_events AS (
    SELECT
        user_id AS shop_subdomain,
        event_id,
        timestamp
    FROM {{ source('mesa_segment', 'flow_events') }}
),

workflow_events AS (
    SELECT * FROM {{ source('mesa_segment', 'workflow_events') }}
),

workflow_event_achievements AS (
    SELECT
        user_id AS shop_subdomain,
        action,
        {{ pacific_timestamp('MIN(timestamp)') }} AS achieved_at_pt
    FROM shops
    INNER JOIN workflow_events ON shops.shop_subdomain = workflow_events.user_id
    GROUP BY 1, 2
),

mesa_flow_achievements AS (
    SELECT
        shop_subdomain,
        event_id AS action,
        {{ pacific_timestamp('MIN(timestamp)') }} AS achieved_at_pt
    FROM shops
    INNER JOIN mesa_flow_events USING (shop_subdomain)
    GROUP BY 1, 2
),

all_achievements AS (
    SELECT shop_subdomain, achieved_at_pt, action AS key, 'workflow_events' AS source
    FROM workflow_event_achievements

    UNION ALL

    SELECT shop_subdomain, achieved_at_pt, action AS key, 'mesa_flow_events' AS source
    FROM mesa_flow_achievements

    UNION ALL

    SELECT
        shop_subdomain,
        first_installed_at_pt AS achieved_at_pt,
        'installed_app' AS key,
        'hardcoded_in_dbt' AS source
    FROM shops
),

funnel_achievements AS (
    SELECT shop_subdomain, funnel_steps.*, achieved_at_pt
    FROM funnel_steps
    LEFT JOIN all_achievements USING (key, source)
    INNER JOIN shops USING (shop_subdomain)
),

max_funnel_steps AS (
    SELECT
        shop_subdomain,
        achieved_at_pt AS max_funnel_step_achieved_at_pt,
        step_order AS max_funnel_step,
        CASE
            WHEN activation_date_pt IS NOT NULL THEN '7-Activated'
            ELSE (step_order || '-' || name)
        END AS max_funnel_step_name,
        COALESCE(step_order, 0) >= 3 AS has_a_workflow,
        COALESCE(step_order, 0) >= 4 AS has_saved_a_workflow,
        COALESCE(step_order, 0) >= 6 AS has_enabled_a_workflow
    FROM shops
    LEFT JOIN funnel_achievements USING (shop_subdomain)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY step_order DESC) = 1
),

{# ========== Shop Infos (from stg_constellation_users) ========== #}

shop_infos AS (
    SELECT
        * EXCLUDE (
            updated_at,
            shopify_createdat,
            analytics_gmv,
            shopify_plandisplayname,
            shopify_inactiveat,
            analytics_orders,
            shopify_planname
        )
    FROM {{ ref("stg_constellation_users") }}
),

{# ========== Cohort Averages ========== #}

cohort_average_current_shop_gmv AS (
    SELECT AVG(shopify_shop_gmv_current_total_usd) AS avg_current_gmv_usd
    FROM shops
),

cohort_average_initial_shop_gmv AS (
    SELECT
        cohort_month,
        AVG(shopify_shop_gmv_initial_total_usd) AS avg_initial_gmv_usd
    FROM shops
    GROUP BY 1
),

{# ========== First Workflow Keys (from int_first_workflow_keys) ========== #}

first_workflows AS (
    SELECT *
    FROM workflows
    WHERE step_count > 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY created_at_pt ASC) = 1
),

first_workflow_first_steps AS (
    SELECT
        shop_subdomain,
        workflow_id AS first_workflow_id,
        integration_app AS first_workflow_trigger_app,
        step_key AS first_workflow_trigger_key,
        operation_id AS first_workflow_trigger_operation_id,
        step_name AS first_workflow_trigger_name,
        workflow_step_id AS first_workflow_trigger_step_id,
        title AS first_workflow_title,
        IFF(is_deleted, 'DELETED - ' || title, title) AS first_workflow_sort_title,
        app_chain AS first_workflow_app_chain,
        step_chain AS first_workflow_step_chain
    FROM first_workflows
    LEFT JOIN workflow_steps USING (workflow_id)
    WHERE workflow_steps.step_type = 'input'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY position_in_workflow DESC) = 1
),

first_workflow_last_steps AS (
    SELECT
        workflow_id AS first_workflow_id,
        integration_app AS first_workflow_destination_app,
        step_key AS first_workflow_destination_key,
        operation_id AS first_workflow_destination_operation_id,
        step_name AS first_workflow_destination_name
    FROM workflow_steps
    WHERE
        step_type = 'output'
        AND workflow_step_id NOT IN (SELECT first_workflow_trigger_step_id FROM first_workflow_first_steps)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY position_in_workflow DESC) = 1
),

first_workflow_keys AS (
    SELECT
        shop_subdomain,
        first_workflow_id,
        first_workflow_trigger_app,
        first_workflow_trigger_key,
        first_workflow_trigger_operation_id,
        first_workflow_trigger_name,
        first_workflow_title,
        first_workflow_sort_title,
        first_workflow_app_chain,
        first_workflow_step_chain,
        first_workflow_destination_app,
        first_workflow_destination_key,
        first_workflow_destination_operation_id,
        first_workflow_destination_name,
        first_workflow_trigger_app || ' - ' || first_workflow_destination_app AS first_workflow_trigger_destination_app_pair,
        first_workflow_trigger_key || ' - ' || first_workflow_destination_key AS first_workflow_trigger_destination_key_pair,
        first_workflow_trigger_name || ' - ' || first_workflow_destination_name AS first_workflow_trigger_destination_name_pair
    FROM first_workflow_first_steps
    LEFT JOIN first_workflow_last_steps USING (first_workflow_id)
),

{# ========== Max Workflow Steps ========== #}

max_workflow_steps AS (
    SELECT
        shop_subdomain,
        COALESCE(MAX(step_count), 0) AS max_workflow_steps,
        COALESCE(MAX(step_count_with_deleted), 0) AS max_workflow_steps_with_deleted
    FROM shops
    LEFT JOIN workflows USING (shop_subdomain)
    GROUP BY 1
),

{# ========== Workflow Source/Destination Pairs ========== #}

workflow_triggers AS (
    SELECT
        workflow_id,
        integration_app AS trigger_app
    FROM workflow_steps
    WHERE step_type = 'input'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY workflow_step_id) = 1
),

workflow_destinations AS (
    SELECT
        workflow_id,
        integration_app AS destination_app
    FROM workflow_steps
    WHERE step_type = 'output'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY workflow_step_id DESC) = 1
),

workflow_pairs AS (
    SELECT
        workflow_id,
        trigger_app || ' - ' || destination_app AS source_destination_pair
    FROM workflow_triggers
    LEFT JOIN workflow_destinations USING (workflow_id)
),

workflow_source_destination_pairs AS (
    SELECT
        shop_subdomain,
        NULLIF(
            LISTAGG(DISTINCT source_destination_pair, ',') WITHIN GROUP (ORDER BY source_destination_pair ASC),
            ''
        ) AS source_destination_pairs_list
    FROM workflows
    LEFT JOIN workflow_pairs USING (workflow_id)
    GROUP BY 1
),

{# ========== Final ========== #}

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
        NOT activation_date_pt IS NULL AS is_activated,
        IFF(is_activated, 'activated', 'onboarding') AS funnel_phase,
        {{ dbt.datediff("first_installed_at_pt::DATE", "activation_date_pt", "days") }} AS days_to_activation,
        COALESCE(has_had_launch_session, NOT launch_session_date IS NULL) AS has_had_launch_session_final,
        {{ dbt.datediff("launch_session_date", "activation_date_pt", "days") }} AS days_from_launch_session_to_activation,
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
        'https://www.theshoppad.com/homeroom.theshoppad.com/admin/backdoor/' || shop_subdomain || '/mesa' AS backdoor_url,
        'https://insights.hotjar.com/sites/1547357/workspaces/1288874/playbacks/list?filters=%7B%22AND%22:%5B%7B%22DAYS_AGO%22:%7B%22created%22:365%7D%7D,%7B%22EQUAL%22:%7B%22user_attributes.str.user_id%22:%22' || shop_subdomain || '%22%7D%7D%5D%7D' AS hotjar_url,
        COALESCE(trial_ends_pt >= CURRENT_DATE, FALSE) AS is_in_trial,
        CASE
            WHEN max_workflow_steps <= 2 THEN 1
            WHEN max_workflow_steps BETWEEN 3 AND 4 THEN 2
            ELSE 3
        END AS virtual_plan_step_qualifier,
        IFF(is_using_pro_apps, 2, 1) AS virtual_plan_pro_app_qualifier,
        GREATEST(virtual_plan_step_qualifier, virtual_plan_pro_app_qualifier) AS virtual_plan
    FROM shops
    LEFT JOIN billing_accounts USING (shop_subdomain)
    LEFT JOIN price_per_actions USING (shop_subdomain)
    LEFT JOIN csm_support USING (shop_subdomain)
    LEFT JOIN shop_workflow_counts USING (shop_subdomain)
    LEFT JOIN max_funnel_steps USING (shop_subdomain)
    LEFT JOIN shop_infos USING (shop_subdomain)
    LEFT JOIN cohort_average_current_shop_gmv
    LEFT JOIN cohort_average_initial_shop_gmv USING (cohort_month)
    LEFT JOIN first_workflow_keys USING (shop_subdomain)
    LEFT JOIN max_workflow_steps USING (shop_subdomain)
    LEFT JOIN shop_integration_app_counts USING (shop_subdomain)
    LEFT JOIN workflow_source_destination_pairs USING (shop_subdomain)
)

SELECT *
FROM final
