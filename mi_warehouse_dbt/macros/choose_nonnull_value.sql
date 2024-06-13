{% macro choose_nonnull_value(first_column_name, second_column_name, out_column_name) %}
    CASE WHEN {{ first_column_name }} IS NULL THEN {{ second_column_name }} ELSE {{ first_column_name }} END AS {{ out_column_name }}
{% endmacro %}