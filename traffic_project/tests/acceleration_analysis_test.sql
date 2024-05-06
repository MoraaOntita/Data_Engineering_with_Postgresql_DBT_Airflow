-- tests/acceleration_analysis_test.sql

-- Macro to perform data quality tests for acceleration_analysis model
{% macro acceleration_analysis_data_quality_tests() %}
    {% test not_null('acceleration') %}
    {% test unique('trajectory_id') %}
{% endmacro %}

-- Perform data quality tests for acceleration_analysis model
{{ acceleration_analysis_data_quality_tests() }}
