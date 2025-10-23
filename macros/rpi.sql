{# --------------------------------------------
# Compact RPI Macro - Produces same compiled output
# -------------------------------------------- #}
{% macro calculate_rpi_increase(
    network, connection_date, claim_month, base_mrc, claim_fy_month
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
        {"fy": "2025-04-01", "cut": "2025-01-31", "r": 1.069},
    ] %}

    {% set rates = o2_rates if network == "'O2'" else vf_rates %}
    {% set is_o2 = network == "'O2'" %}
    {% set vf_post = (
        "CONNECTIONDATE >= to_date('2024-07-02')" if not is_o2 else "1=0"
    ) %}

    {# Main calculation #}
    round(
        case
            when
                {{ network }} = 'Vodafone'
                and {{ connection_date }} >= to_date('2024-07-02')
            then
                {{ base_mrc }} + case
                    when {{ claim_fy_month }} >= to_date('2025-04-01')
                    then
                        floor(
                            datediff(month, to_date('2025-04-01'), {{ claim_fy_month }})
                            / 12
                        )
                        * 1.8
                        + 1.8
                    else 0
                end
            else
                {{ base_mrc }}
                {%- for rate in rates %}
                    * case
                        when
                            {{ connection_date }} < to_date('{{ rate.cut }}')
                            and {{ claim_fy_month }} >= to_date('{{ rate.fy }}')
                        then {{ rate.r }}
                        else 1.0
                    end
                {%- endfor %}
                {%- if is_o2 %}
                    + case
                        when {{ claim_fy_month }} >= to_date('2025-04-01')
                        then
                            floor(
                                datediff(
                                    month, to_date('2025-04-01'), {{ claim_fy_month }}
                                )
                                / 12
                            )
                            * 1.8
                            + 1.8
                        else 0
                    end
                {%- endif %}
        end,
        2
    )

{% endmacro %}
