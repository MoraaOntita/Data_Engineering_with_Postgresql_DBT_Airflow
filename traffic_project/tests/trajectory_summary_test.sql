/*
This file contains data quality tests for the trajectory_summary model.
These tests ensure that critical columns do not contain null values and that primary keys are unique.
*/

-- Perform data quality tests for trajectory_summary model
{% test not_null(column='distance') %}
-- This test ensures that the 'distance' column does not contain null values,
-- as distance is a critical metric for the trajectory_summary model.

{% test unique(column='trajectory_id') %}
-- This test ensures that the 'trajectory_id' column is unique,
-- maintaining data integrity and ensuring each trajectory has a unique identifier.

