WITH
shops AS (
    SELECT
        shop_id,
        shop_subdomain
    FROM {{ ref('stg_shops') }}
),

workflows AS (
    SELECT
        *,
        uuid AS shop_subdomain,
        __hevo__marked_deleted AS is_deleted
    FROM {{ source('mesa_mongo', 'workflows') }}
),

final AS (
    SELECT
        _id AS workflow_id,
        shop_id,
        shop_subdomain,
        {# source AS first_step_app, #}
        {# destination AS last_step_app, TODO: Change to use Steps https://shoppad.slack.com/archives/D01UTNZKM6D/p1667343343556039 #}
        template AS template_name
    FROM workflows
    INNER JOIN shops USING (shop_subdomain) -- Filter out any workflows that don't belong to a shop.
    WHERE NOT(is_deleted)
)

SELECT * FROM final
