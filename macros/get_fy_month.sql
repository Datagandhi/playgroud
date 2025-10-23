{% macro get_fy_month(claim_month) %}
    to_date(
        case when month({{ claim_month }}) >= 4
            then to_char(year({{ claim_month }})) || '-04-01'
            else to_char(year({{ claim_month }})-1) || '-04-01'
        end
    )
{% endmacro %}