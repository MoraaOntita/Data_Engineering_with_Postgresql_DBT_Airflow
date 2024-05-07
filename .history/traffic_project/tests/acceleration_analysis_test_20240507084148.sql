/*
This file contains data quality tests for the acceleration_analysis model.
These tests ensure that critical columns do not contain null values and that primary keys are unique.
*/

-- Macro to perform data quality tests for acceleration_analysis model
{% macro acceleration_analysis_data_quality_tests() %}
    {% test not_null('acceleration') %}
    -- This test ensures that the 'acceleration' column does not contain null values,
    -- as acceleration is a critical metric for the acceleration_analysis model.

    {% test unique('trajectory_id') %}
    -- This test ensures that the 'trajectory_id' column is unique,
    -- maintaining data integrity and ensuring each trajectory has a unique identifier.
{% endmacro %}

-- Perform data quality tests for acceleration_analysis model
{{ acceleration_analysis_data_quality_tests() }}
