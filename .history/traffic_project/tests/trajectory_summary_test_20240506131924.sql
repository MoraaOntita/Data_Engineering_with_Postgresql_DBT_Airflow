-- tests/trajectory_summary_test.sql

-- Macro to perform data quality tests for trajectory_summary model
{% macro trajectory_summary_data_quality_tests() %}
    {% test not_null('distance') %}
    {% test unique('trajectory_id') %}
{% endmacro %}

-- Perform data quality tests for trajectory_summary model
{{ trajectory_summary_data_quality_tests() }}
