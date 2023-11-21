{%- set constellation_apps = [
    "blogstudio",
    "bouncer",
    "uploadery",
    "infinite_options",
    "coin",
    "tracktor",
    "pagestudio",
    "smile",
    "kitkarts",
] -%}

with
    conversion_rates as (
        select currency as analytics_currency, in_usd
        from {{ ref("currency_conversion_rates") }}
    ),

    constellation_users as (
        select
            coalesce(uuid, id) as shop_subdomain,
            createdat as first_in_constellation_at_utc,
            {{ pacific_timestamp("first_in_constellation_at_utc") }}
            as first_in_constellation_at_pt,
            first_in_constellation_at_pt::date as first_in_constellation_on_pt,
            date_trunc(
                'week', first_in_constellation_on_pt
            ) as constellation_cohort_week,
            coalesce(updatedat, createdat, uuid_ts) as updated_at,
            1.0 * analytics_gmv * in_usd as analytics_gmv,
            analytics_orders,
            shopify_createdat,
            shopify_inactiveat,
            shopify_plandisplayname,
            analytics_currency,
            shopify_planname,
            support_lastreplyat,
            coalesce(support_lastreplyat is not null, false) as has_contacted_support,
            coalesce((1.0 * analytics_gmv * in_usd) > 3000, false)
            or shopify_planname
            in ('professional', 'unlimited', 'shopify_plus') as is_mql,
            {%- for app in constellation_apps %}
                coalesce(
                    coalesce(
                        apps_
                        {% if app == "infinite_options" %} customizery
                        {% else %} {{ app }}
                        {% endif %} _isactive,
                        apps_
                        {% if app == "infinite_options" %} customizery
                        {% else %} {{ app }}
                        {% endif %} _installedat is not null
                    ),
                    false
                ) as has_{{ app }},
                coalesce(
                    apps_
                    {% if app == "infinite_options" %} customizery
                    {% else %} {{ app }}
                    {% endif %} _installedat is not null,
                    false
                ) as has_ever_installed_{{ app }}
                {%- if not loop.last %}, {% endif -%}
            {%- endfor %},
            coalesce(
                {%- for app in constellation_apps -%}
                    has_{{ app }} {%- if not loop.last %} or {% endif -%}
                {% endfor %},
                false
            ) as has_shoppad_constellation_app,

            coalesce(
                {%- for app in constellation_apps %}
                    apps_
                    {% if app == "infinite_options" %} customizery
                    {% else %} {{ app }}
                    {% endif %} _installedat < apps_mesa_installedat
                    {%- if not loop.last %} or {% endif -%}
                {% endfor %},
                false
            ) as did_install_another_shoppad_app_first,
            coalesce(did_install_another_shoppad_app_first, false) as is_pql,
            case
                {% for app in constellation_apps -%}
                    when
                        apps_
                        {% if app == "infinite_options" %} customizery
                        {% else %} {{ app }}
                        {% endif %} _installedat < apps_mesa_installedat
                    then '{{ app }}'
                {% endfor -%}
                else 'mesa'
            end as first_shoppad_app_installed,

            least(
                {%- for app in constellation_apps %}
                    coalesce(
                        apps_
                        {% if app == "infinite_options" %} customizery
                        {% else %} {{ app }}
                        {% endif %} _installedat,
                        current_timestamp()
                    )
                    {%- if not loop.last %}, {% endif -%}
                {% endfor %},
                coalesce(apps_mesa_installedat, current_timestamp())
            ) as first_shoppad_app_installed_at_utc,
            {{ pacific_timestamp("first_shoppad_app_installed_at_utc") }}
            as first_shoppad_app_installed_at_pt,
            array_to_string(
                array_sort(split(apps_mesa_meta_appsused_value, ',')), ','
            ) as apps_used,
            array_to_string(
                array_sort(array_except(split(apps_used, ','), {{ var("glue_apps") }})),
                ','
            ) as apps_used_without_glue
        from {{ source("php_segment", "users") }}
        left join conversion_rates using (analytics_currency)
        qualify
            row_number() over (partition by coalesce(uuid, id) order by createdat desc)
            = 1
    )

select *
from constellation_users
