{% macro calculate_rpi_increase(
    network, connection_date, claim_month, base_mrc, claim_start_of_fy
) %}

    {# Rate definitions #}
    {% set o2_rates = [
        {"fy": "2022-04-01", "cut": "2022-04-01", "r": 1.117},
        {"fy": "2023-04-01", "cut": "2023-04-01", "r": 1.173},
        {"fy": "2024-04-01", "cut": "2024-04-01", "r": 1.088},
    ] %}

    {% set vf_rates = [
        {"fy": "2022-04-01", "cut": "2022-01-31", "r": 1.093},
        {"fy": "2023-04-01", "cut": "2023-01-31", "r": 1.144},
        {"fy": "2024-04-01", "cut": "2024-01-31", "r": 1.079},
        {"fy": "2025-04-01", "cut": "2025-01-31", "r": 1.064},
    ] %}

    {% set rates = o2_rates if network == "'O2'" else vf_rates %}
    {% set is_o2 = network == "'O2'" %}

    {# Main calculation #}
    round(
        {{ base_mrc }}
        {%- for rate in rates %}
            * case
                when
                    {{ connection_date }} < to_date('{{ rate.cut }}')
                    and {{ claim_start_of_fy }} >= to_date('{{ rate.fy }}')
                then {{ rate.r }}
                else 1.0
            end
        {%- endfor %} + case
            when
                {% if not is_o2 %}
                    {{ connection_date }} >= to_date('2024-07-02') and
                {% endif %} {{ claim_start_of_fy }}
                >= to_date('2025-04-01')
            then
                floor(datediff(month, to_date('2025-04-01'), {{ claim_start_of_fy }}) / 12)
                * 1.8
                + 1.8
            else 0
        end,
        2
    )

{% endmacro %}


