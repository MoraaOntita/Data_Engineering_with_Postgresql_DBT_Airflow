-- Perform data quality tests for trajectory_summary model
{% not_null(column='distance') %}
-- This test ensures that the 'distance' column does not contain null values,
-- as distance is a critical metric for the trajectory_summary model.

{% unique(column='trajectory_id') %}
-- This test ensures that the 'trajectory_id' column is unique,
-- maintaining data integrity and ensuring each trajectory has a unique identifier.


