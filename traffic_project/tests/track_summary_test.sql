-- tests/track_summary_test.sql

-- Macro to perform data quality tests for track_summary model
{% macro track_summary_data_quality_tests() %}
    {% test not_null('traveled_d') %}
    {% test unique('track_id') %}
{% endmacro %}

-- Perform data quality tests for track_summary model
{{ track_summary_data_quality_tests() }}
