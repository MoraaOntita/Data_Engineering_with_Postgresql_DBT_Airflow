/*
This file contains data quality tests for the track_summary model.
These tests ensure that critical columns do not contain null values and that primary keys are unique.
*/

-- Macro to perform data quality tests for track_summary model
{% macro track_summary_data_quality_tests() %}
    {% test not_null('traveled_d') %}
    -- This test ensures that the 'traveled_d' column does not contain null values,
    -- as distance traveled is a critical metric for the track_summary model.

    {% test unique('track_id') %}
    -- This test ensures that the 'track_id' column is unique,
    -- maintaining data integrity and ensuring each track has a unique identifier.
{% endmacro %}

-- Perform data quality tests for track_summary model
{{ track_summary_data_quality_tests() }}
