{% macro days_in_month(month_date) -%}
  date_part('days', (date_trunc('month', {{ month_date }}::date) + interval '1 month - 1 day'))::int
{%- endmacro %}
