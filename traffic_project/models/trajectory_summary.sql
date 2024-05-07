/*
This model calculates summary statistics for each track's trajectory based on data from the df_trajectory table.
It calculates the number of points, total distance traveled, and average speed for each track.
Input: df_trajectory (source table containing trajectory information)
Output: summary (table containing aggregated statistics for each track's trajectory)
*/

{{ config(materialized='table') }}

-- Define a CTE to calculate summary statistics for each track's trajectory
WITH trajectory_summary_stats AS (
    SELECT
        track_id,
        COUNT(*) AS num_points,
        SUM(distance) AS total_distance,
        AVG(speed) AS avg_speed
    FROM
        {{ ref('df_trajectory') }}
    GROUP BY
        track_id
)

-- Define the main model
SELECT
    track_id, -- Identifier for the track
    num_points, -- Number of points in the trajectory
    total_distance, -- Total distance traveled by the track's trajectory
    avg_speed -- Average speed of the track's trajectory

-- Define expectations for the output
{% not_null('track_id') %}
{% not_null('num_points') %}
{% not_null('total_distance') %}
{% not_null('avg_speed') %}

FROM
    {{ trajectory_summary_stats() }} summary;

