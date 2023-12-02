with

    installation_events as (
        select
            user_id as shop_subdomain,
            {{ pacific_timestamp("timestamp") }} as event_timestamp_pt
        from {{ source("mesa_segment", "install_events") }}
    )

select *
from installation_events
