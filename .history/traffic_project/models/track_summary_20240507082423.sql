-- models/track_summary.sql

/*
This model calculates summary statistics for each track based on data from the df_track table.
It calculates total distance traveled, average speed, and maximum speed for each track.
Input: df_track (source table containing track information)
Output: summary (table containing aggregated statistics for each track)
*/

{{ config(materialized='table') }}

-- Define a CTE to calculate summary statistics for each track
WITH track_summary_stats AS (
    SELECT
        track_id,
        type,
        SUM(traveled_d) AS total_distance,
        AVG(avg_speed) AS avg_speed,
        MAX(avg_speed) AS max_speed
    FROM
        {{ ref('df_track') }}
    GROUP BY
        track_id, type
)

-- Define a macro to add prefix to column names
{% macro prefixed_columns(prefix, columns) -%}
    {{- join([prefix ~ '.' ~ column for column in columns], ', ') -}}
{%- endmacro %}

-- Define a macro to use for re-usable CTEs
{% macro track_summary_summary_stats() -%}
    SELECT
        track_id,
        type,
        total_distance,
        avg_speed,
        max_speed
    FROM
        track_summary_stats
{%- endmacro %}

-- Define the main model
SELECT
    track_id, -- Identifier for the track
    type, -- Type of vehicle or entity (e.g., Car, Motorcycle)
    total_distance, -- Total distance traveled by the track
    avg_speed, -- Average speed of the track
    max_speed -- Maximum speed of the track
FROM
    {{ track_summary_summary_stats() }} summary;
