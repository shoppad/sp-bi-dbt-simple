with
    shop_anonymous_keys as (
        select * from {{ ref("stg_anonymous_to_known_user_matching") }}
    ),

    installation_events as (
        select
            user_pseudo_id,
            {{ pacific_timestamp("TO_TIMESTAMP(event_timestamp)") }}
            as event_timestamp_pt
        from {{ source("mesa_ga4", "events") }}
        where event_name = 'getmesa_install_convert'
        qualify
            row_number() over (
                partition by user_pseudo_id, event_timestamp
                order by param_source, name, __hevo__loaded_at
            )
            = 1

    )
select *
from installation_events
inner join shop_anonymous_keys using (user_pseudo_id)
