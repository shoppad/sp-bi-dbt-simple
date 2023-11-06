with
    user_id_matches as (
        select user_pseudo_id, user_id as shop_subdomain
        from {{ source("mesa_ga4", "events") }}
        where
            event_name = 'install_stage_permissions_accept'
            and shop_subdomain is not null
        qualify
            row_number() over (
                partition by user_pseudo_id
                order by event_timestamp, param_source, name, __hevo__loaded_at
            )
            = 1

    )
select *
from user_id_matches
