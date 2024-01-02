with

    installation_events as (
        select
            *
        from {{ ref("int_ga4_events") }}
        where event_name = 'getmesa_install_convert'
    )

select
    *
from installation_events

qualify
    row_number() over (
        partition by shop_subdomain order by event_timestamp_pt asc
    )
    = 1
