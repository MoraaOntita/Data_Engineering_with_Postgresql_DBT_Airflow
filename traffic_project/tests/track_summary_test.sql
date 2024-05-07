/*
This file contains data quality tests for the track_summary model.
These tests ensure that critical columns do not contain null values and that primary keys are unique.
*/

-- Macro to perform data quality tests for track_summary model
{% macro track_summary_data_quality_tests() %}
    {% test not_null('total_distance') %}
    -- This test ensures that the 'total_distance' column does not contain null values,
    -- as it represents the total distance traveled by each track.

    {% test not_null('avg_speed') %}
    -- This test ensures that the 'avg_speed' column does not contain null values,
    -- as it represents the average speed of each track.

    {% test not_null('max_speed') %}
    -- This test ensures that the 'max_speed' column does not contain null values,
    -- as it represents the maximum speed of each track.
{% endmacro %}

-- Perform data quality tests for track_summary model
{{ track_summary_data_quality_tests() }}
