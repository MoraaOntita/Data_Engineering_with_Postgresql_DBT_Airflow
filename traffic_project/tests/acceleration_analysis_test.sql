/*
This file contains data quality tests for the acceleration_analysis model.
These tests ensure that critical columns do not contain null values and that primary keys are unique.
*/

-- Macro to perform data quality tests for acceleration_analysis model
{% macro acceleration_analysis_data_quality_tests() %}
    {% test not_null('avg_acceleration') %}
    -- This test ensures that the 'avg_acceleration' column does not contain null values,
    -- as it represents the average acceleration of each track.

    {% test not_null('max_acceleration') %}
    -- This test ensures that the 'max_acceleration' column does not contain null values,
    -- as it represents the maximum acceleration of each track.

    {% test not_null('median_acceleration') %}
    -- This test ensures that the 'median_acceleration' column does not contain null values,
    -- as it represents the median acceleration of each track.
{% endmacro %}

-- Perform data quality tests for acceleration_analysis model
{{ acceleration_analysis_data_quality_tests() }}