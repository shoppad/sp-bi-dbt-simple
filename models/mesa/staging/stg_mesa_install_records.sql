WITH source AS (
    SELECT
        * RENAME uuid AS shop_subdomain,
        ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY created_at DESC) AS row_num
    FROM {{ source("mongo_sync", "mesa_install_records") }}
    WHERE
        install_completed
        AND
        uuid IS NOT NULL
),

final AS (

    select
        shop_subdomain,
        {{ get_prefixed_columns(source("mongo_sync", "mesa_install_records"), 'mesa_install_record',
            exclude=[
                "template",
                "state",
                'uuid',
                "mesa_id",
                "__hevo__ingested_at",
                "__hevo__loaded_at",
                "__hevo__database_name",
                "updated_at",
                "utm_term",
                "route",
                "ip",
                "__hevo__marked_deleted",
                "_created_at",
                "_id",
                "session"
            ],
        ) }},
        template as acquisition_template
    FROM source
    WHERE row_num = 1
)

SELECT *
FROM final
