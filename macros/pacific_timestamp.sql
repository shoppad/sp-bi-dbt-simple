{%- macro pacific_timestamp(timestamp_str) -%}
    {{ dbt_date.convert_timezone(timestamp_str, "America/Los_Angeles", 'UTC') }}
{%- endmacro -%}