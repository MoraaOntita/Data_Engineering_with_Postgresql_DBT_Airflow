-- models/trajectory_summary.sql
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

-- Define a macro to add prefix to column names
{% macro prefixed_columns(prefix, columns) -%}
    {{- join([prefix ~ '.' ~ column for column in columns], ', ') -}}
{%- endmacro %}

-- Define a macro to use for re-usable CTEs
{% macro trajectory_summary_stats() -%}
    SELECT
        track_id,
        num_points,
        total_distance,
        avg_speed
    FROM
        trajectory_summary_stats
{%- endmacro %}

-- Define the main model
SELECT
    {{ prefixed_columns('summary', [
        'track_id',
        'num_points',
        'total_distance',
        'avg_speed'
    ]) }}
FROM
    {{ trajectory_summary_stats() }} summary;
