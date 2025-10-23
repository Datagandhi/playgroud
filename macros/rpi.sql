{# --------------------------------------------
# Main macro - Unified RPI Calculation
# -------------------------------------------- #}
{% macro calculate_rpi_increase(
    network, connection_date, claim_month, base_mrc, claim_fy_month
) %}

    {# ---------- Define Rate Rules ---------- #}
    
    {# O2: Uses FY start as cutoff #}
    {% set o2_rates = [
        {"fy_start": "2022-04-01", "cutoff": "2022-04-01", "rate": 1.117},
        {"fy_start": "2023-04-01", "cutoff": "2023-04-01", "rate": 1.173},
        {"fy_start": "2024-04-01", "cutoff": "2024-04-01", "rate": 1.088}
    ] %}
    
    {# Vodafone: Uses Jan 31 cutoff for April increase #}
    {% set vf_rates = [
        {"fy_start": "2022-04-01", "cutoff": "2022-01-31", "rate": 1.093},
        {"fy_start": "2023-04-01", "cutoff": "2023-01-31", "rate": 1.144},
        {"fy_start": "2024-04-01", "cutoff": "2024-01-31", "rate": 1.079},
        {"fy_start": "2025-04-01", "cutoff": "2025-01-31", "rate": 1.069}
    ] %}
    
    {# Select appropriate rate table #}
    {% set rate_table = o2_rates if network == "'O2'" else vf_rates %}
    
    {# Vodafone transition date #}
    {% set vf_transition = "2024-07-02" %}
    
    {# ---------- Calculate Adjusted MRC ---------- #}
    
    {% set calculation %}
    round(
        case
            {# Vodafone post-transition: skip percentage logic, go straight to fixed adds #}
            when {{ network }} = 'Vodafone' 
                 and {{ connection_date }} >= to_date('{{ vf_transition }}')
            then
                {{ base_mrc }}
                + case
                    when {{ claim_fy_month }} >= to_date('2025-04-01')
                    then (
                        floor(datediff(month, to_date('2025-04-01'), {{ claim_fy_month }}) / 12) * 1.8
                        + 1.8
                    )
                    else 0
                  end
            
            {# Standard logic: O2 or Vodafone pre-transition #}
            else
                {{ base_mrc }}
                {# Apply cumulative percentage increases #}
                {% for r in rate_table %}
                * case 
                    when {{ connection_date }} < to_date('{{ r.cutoff }}')
                         and {{ claim_fy_month }} >= to_date('{{ r.fy_start }}')
                    then {{ r.rate }}
                    else 1.0
                  end
                {% endfor %}
                {# Add fixed £1.80 from Apr 2025 for O2 (Vodafone pre-transition doesn't get this) #}
                {% if network == "'O2'" %}
                + case
                    when {{ claim_fy_month }} >= to_date('2025-04-01')
                    then (
                        floor(datediff(month, to_date('2025-04-01'), {{ claim_fy_month }}) / 12) * 1.8
                        + 1.8
                    )
                    else 0
                  end
                {% endif %}
        end
    , 2)
    {% endset %}
    
    {{ calculation }}

{% endmacro %}