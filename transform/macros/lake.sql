{% macro lake(path) -%}
  read_parquet('{{ var("lake_path") }}/{{ path }}', union_by_name = true, filename = true)
{%- endmacro %}
