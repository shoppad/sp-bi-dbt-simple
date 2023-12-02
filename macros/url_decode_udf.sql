{% macro url_decode_udf() %}

    create or replace function {{ target.schema }}.url_decode(input string)
    returns string
    language javascript
    as '
    var decoded = decodeURIComponent(INPUT);
    return decoded.replace(/\\+/g, " ");
'
    ;
{% endmacro %}
