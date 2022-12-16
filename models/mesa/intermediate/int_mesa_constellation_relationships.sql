{%- set constellation_apps = ['blog_studio', 'bouncer', 'uploadery', 'infinite_options', 'coin', 'tracktor', 'page_studio', 'smile', 'kitkarts'] -%}

WITH
{% for app in constellation_apps %}
    {{ app }}_shops AS (
        SELECT
            DISTINCT uuid AS shop_subdomain,
            TRUE as has_{{ app }}
        {# FROM {{ ref('stg_' + app + '_shops') }} #}
        FROM {{ source('mongo_sync',  app + '_shops') }}
    ),
{% endfor %}

final AS (
    SELECT
        shop_subdomain,
        {%- for app in constellation_apps -%}
            IFNULL(has_{{ app }}, FALSE) as has_{{ app }}
            {%- if not loop.last %}, {% endif %}
        {% endfor -%}
    FROM {{ ref('int_shops') }} {# TODO: This model should be renamed to mesa_shops. #}
    {%- for app in constellation_apps -%}
        LEFT JOIN {{ app + '_shops' }} USING (shop_subdomain)
    {% endfor -%}
)

SELECT * FROM final
