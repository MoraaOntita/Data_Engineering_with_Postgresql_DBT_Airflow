/*
This model calculates summary statistics for speed analysis based on data from the df_track table.
It calculates the average speed and maximum speed for each track.
Input: df_track (source table containing track information)
Output: summary (table containing aggregated statistics for speed analysis)
*/

{{ config(materialized='table') }}

-- Define a CTE to calculate summary statistics for speed analysis
WITH speed_summary_stats AS (
    SELECT
        track_id,
        AVG(avg_speed) AS avg_speed,
        MAX(avg_speed) AS max_speed
    FROM
        {{ ref('df_track') }}
    GROUP BY
        track_id
)

-- Define a macro to add prefix to column names
{% macro prefixed_columns(prefix, columns) -%}
    {{- join([prefix ~ '.' ~ column for column in columns], ', ') -}}
{%- endmacro %}

-- Define a macro to use for re-usable CTEs
{% macro speed_summary_stats() -%}
    SELECT
        track_id,
        avg_speed,
        max_speed
    FROM
        speed_summary_stats
{%- endmacro %}

-- Define the main model
SELECT
    track_id, -- Identifier for the track
    avg_speed, -- Average speed of the track
    max_speed -- Maximum speed of the track
FROM
    {{ speed_summary_stats() }} summary;
