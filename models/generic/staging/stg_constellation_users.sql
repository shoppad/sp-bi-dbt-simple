{%- set constellation_apps = ['blogstudio', 'bouncer', 'uploadery', 'infinite_options', 'coin', 'tracktor', 'pagestudio', 'smile', 'kitkarts'] -%}

WITH conversion_rates AS (
    SELECT
        currency AS analytics_currency,
        in_usd
    FROM {{ ref('currency_conversion_rates') }}
),

constellation_users AS (
    SELECT
        COALESCE(uuid, id) AS shop_subdomain,
        createdat AS first_in_constellation_at_utc,
        {{ pacific_timestamp('first_in_constellation_at_utc') }} AS first_in_constellation_at_pt,
        first_in_constellation_at_pt::DATE AS first_in_constellation_on_pt,
        date_trunc('week', first_in_constellation_on_pt) AS constellation_cohort_week,
        COALESCE(updatedat, createdat, uuid_ts) AS updated_at,
         1.0 * analytics_gmv * in_usd AS analytics_gmv,
        analytics_orders,
        shopify_createdat,
        shopify_inactiveat,
        shopify_plandisplayname,
        analytics_currency,
        shopify_planname,
        support_lastreplyat,
        COALESCE(support_lastreplyat IS NOT NULL, FALSE) AS has_contacted_support,
        COALESCE(analytics_gmv > 3000, FALSE) OR shopify_planname IN ('professional', 'unlimited', 'shopify_plus') AS is_mql,
        {%- for app in constellation_apps %}
            COALESCE(COALESCE(apps_{% if app == "infinite_options" %}customizery{% else %}{{ app }}{% endif %}_isactive, apps_{% if app == "infinite_options" %}customizery{% else %}{{ app }}{% endif %}_installedat IS NOT NULL), FALSE) AS has_{{ app }},
            COALESCE(apps_{% if app == "infinite_options" %}customizery{% else %}{{ app }}{% endif %}_installedat IS NOT NULL, FALSE) AS has_ever_installed_{{ app }}{%- if not loop.last %}, {% endif -%}
        {%- endfor %},
        COALESCE({%- for app in constellation_apps -%}
            has_{{ app }} {%- if not loop.last %} OR {% endif -%}
        {% endfor %}, FALSE) AS has_shoppad_constellation_app,

        COALESCE({%- for app in constellation_apps %}
         apps_{% if app == "infinite_options" %}customizery{% else %}{{ app }}{% endif %}_installedat < apps_mesa_installedat {%- if not loop.last %} OR {% endif -%}
        {% endfor %}, FALSE) AS did_install_another_shoppad_app_first,
        COALESCE(did_install_another_shoppad_app_first, FALSE) AS is_pql,
        CASE
        {% for app in constellation_apps -%}
            WHEN apps_{% if app == "infinite_options" %}customizery{% else %}{{ app }}{% endif %}_installedat < apps_mesa_installedat THEN '{{ app }}'
        {% endfor -%}
        ELSE 'mesa'
        END AS first_shoppad_app_installed,


        LEAST({%- for app in constellation_apps %}
            COALESCE(apps_{% if app == "infinite_options" %}customizery{% else %}{{ app }}{% endif %}_installedat, current_timestamp()){%- if not loop.last %}, {% endif -%}
        {% endfor %}, COALESCE(apps_mesa_installedat, current_timestamp())) AS first_shoppad_app_installed_at_utc,
        {{ pacific_timestamp('first_shoppad_app_installed_at_utc') }} AS first_shoppad_app_installed_at_pt,
        apps_mesa_meta_appsused_value AS apps_used,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    ',' || apps_used || ',',
                    ',({%- for app in var('glue_apps') -%}{{ app }}{%- if not loop.last %}|{% endif -%}{% endfor %})(,|$)',
                    ','
                ),
                ',,',
                ','
            ),
            ','
        ) AS apps_used_without_glue
    FROM {{ source('php_segment', 'users') }}
    LEFT JOIN conversion_rates USING (analytics_currency)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(uuid, id) ORDER BY createdat DESC) = 1
)

SELECT *
FROM constellation_users
