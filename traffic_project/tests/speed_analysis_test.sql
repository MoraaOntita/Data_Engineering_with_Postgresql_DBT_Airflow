-- tests/speed_analysis_test.sql

-- Macro to perform data quality tests for speed_analysis model
{% macro speed_analysis_data_quality_tests() %}
    {% test not_null('avg_speed') %}
    {% test unique('track_id') %}
{% endmacro %}

-- Perform data quality tests for speed_analysis model
{{ speed_analysis_data_quality_tests() }}
