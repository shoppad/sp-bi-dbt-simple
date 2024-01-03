with

    shops as (select shop_subdomain, first_installed_at_pt from {{ ref("stg_shops") }}),

    {# TODO: Check *all* sessions for an app_store_ad session or pageview and mark is_app_store_ad_acqusition #}
    session_counts as (
        select
            shop_subdomain,
            coalesce(
                count_if(
                    event_timestamp_pt <= first_installed_at_pt + interval '60min'
                ),
                0
            ) as ga4_sessions_til_install
        from shops
        left join {{ ref("int_ga_session_starts") }} using (shop_subdomain)
        group by 1
    ),

    first_touches_ga4 as (select * from {{ ref("int_ga_first_visits") }}),

    last_touches_ga4 as (
        select * from {{ ref("int_last_touch_ga_sessions_before_install") }}
    ),

    combined as (

        select *
        FROM first_touches_ga4
        left join last_touches_ga4 using (shop_subdomain)
        left join session_counts using (shop_subdomain)
    ),

final AS (
   SELECT *
    REPLACE (
        {%- set reformatted_fields = [] -%}
            {%- for prefix in ['first_touch', 'last_touch'] -%}
                {%- for midfix in ['traffic_source', 'manual', 'param'] -%}
                    {%- for endfix in ['source', 'medium', 'campaign_name', 'term', 'content' ] -%}
                        {%- if (endfix=='campaign_name') -%}
                            {% if midfix=='traffic_source' %}
                                {% set endfix = 'name' %}
                            {%- elif midfix=='param' %}
                                {% set endfix = 'campaign' %}
                            {%- endif %}
                        {%- elif (midfix=='traffic_source' and (endfix=='term' or endfix=='content')) -%}
                            {%- continue -%}
                        {%- endif -%}
                        {%- set column_name = [prefix, midfix, endfix] | join('_') -%}
                        {%- do reformatted_fields.append("initcap(replace(" ~ column_name ~ ", '_', ' ')) as " ~ column_name) -%}
                    {% endfor %}
                {% endfor %}
            {% endfor %}
        {{ reformatted_fields | join(',\n       ') }}
    )
    FROM combined
)

select *
from final
