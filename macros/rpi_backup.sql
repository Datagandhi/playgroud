{# --------------------------------------------
# Top-level helper
# -------------------------------------------- #}
{% macro _apply_factor_backup(expr, cond_sql, factor) -%}
    ({{ expr }} * case when {{ cond_sql }} then {{ factor }} else 1 end)
{%- endmacro %}

{# --------------------------------------------
# Main macro
# -------------------------------------------- #}
{% macro calculate_rpi_increase_backup(
    network, connection_date, claim_month, base_mrc, claim_fy_month
) %}
    {# ---------- helpers ---------- #}
    {# Fixed adds as monthly accumulator: £0.15 per eligible month (inclusive of start month) #}
    {% set fixed_add_expr -%}
case
  when {{ claim_month }} >= to_date('2025-04-01')
  then
    -- Add £1.80 for each complete FY since Apr 2025, plus flat £0.15 for current FY
    (floor(datediff(month, to_date('2025-04-01'), date_trunc('month', {{ claim_month }})) / 12) * 1.8) + 0.15
  else 0
end
    {%- endset %}

    {# ---------- rule tables ---------- #}
    {% set o2_year_rules = [
        {"fy_start": "2022-04-01", "factor": 0.117},
        {"fy_start": "2023-04-01", "factor": 0.173},
        {"fy_start": "2024-04-01", "factor": 0.088},
    ] %}

    {% set vf_year_rules = [
        {"fy_start": "2022-04-01", "cutoff": "2022-01-31", "factor": 0.093},
        {"fy_start": "2023-04-01", "cutoff": "2023-01-31", "factor": 0.144},
        {"fy_start": "2024-04-01", "cutoff": "2024-01-31", "factor": 0.079},
        {"fy_start": "2025-04-01", "cutoff": "2025-01-31", "factor": 0.069},
    ] %}

    {# ---------- Percentage increase helper (applied monthly, compounds annually) ---------- #}
    {% if network == "'O2'" %}
        {% set o2_expr %}
        ({{ base_mrc }}
        {%- for r in o2_year_rules %}
        + case when {{ connection_date }} < to_date('{{ r.fy_start }}') and {{ claim_fy_month }} >= to_date('{{ r.fy_start }}') and {{ claim_fy_month }} < to_date('2025-04-01')
          then (floor(datediff(month, to_date('{{ r.fy_start }}'), date_trunc('month', {{ claim_month }})) / 12) * {{ base_mrc }} * {{ r.factor }}) 
               + (({{ base_mrc }} + (floor(datediff(month, to_date('{{ r.fy_start }}'), date_trunc('month', {{ claim_month }})) / 12) * {{ base_mrc }} * {{ r.factor }})) * {{ r.factor }} / 12)
          else 0 end
        {%- endfor %}
        + {{fixed_add_expr}}
        )
        {% endset %}
    {% else %} {% set o2_expr = "(" ~ base_mrc ~ " + " ~ fixed_add_expr ~ ")" %}
    {% endif %}

    {# ---------- Vodafone (< 2024-07-02): cumulative % with cut-offs (no fixed adds) ---------- #}
    {% set vf_pre_expr %}{{ base_mrc }}{% endset %}
    {% for r in vf_year_rules %}
        {% set vf_cond -%}
        {{ connection_date }} <= to_date('{{ r.cutoff }}')
        and {{ claim_fy_month }} >= to_date('{{ r.fy_start }}')
        {%- endset %}
        {% set vf_pre_expr %}{{ _apply_factor(vf_pre_expr, vf_cond, r.factor) }}{% endset %}
    {% endfor %}

    {# ---------- Vodafone (>= 2024-07-02): monthly fixed adds only ---------- #}
    {% set vf_post_expr %}( {{ base_mrc }} + {{ fixed_add_expr }} ){% endset %}

    {# ---------- final ---------- #}
    case
        when {{ network }} = 'O2'
        then {{ o2_expr }}
        when {{ network }} = 'Vodafone'
        then
            case
                when {{ connection_date }} < to_date('2024-07-02')
                then {{ vf_pre_expr }}
                else {{ vf_post_expr }}
            end
        else {{ base_mrc }}
    end
{% endmacro %}