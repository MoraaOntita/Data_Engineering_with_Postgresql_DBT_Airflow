-- models/speed_analysis.sql
{{ config(materialized='view') }}

-- Define a CTE to calculate speed statistics for each track
WITH track_speed_stats AS (
    SELECT
        track_id,
        AVG(avg_speed) AS avg_speed,
        MAX(avg_speed) AS max_speed,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_speed) AS median_speed
    FROM
        {{ ref('track_summary') }} -- Referring to the track_summary model
    GROUP BY
        track_id
)

-- Define a macro to add prefix to column names
{% macro prefixed_columns(prefix, columns) -%}
    {{- join([prefix ~ '.' ~ column for column in columns], ', ') -}}
{%- endmacro %}

-- Define a macro to use for re-usable CTEs
{% macro track_speed_stats() -%}
    SELECT
        track_id,
        avg_speed,
        max_speed,
        median_speed
    FROM
        track_speed_stats
{%- endmacro %}

-- Define the main model
SELECT
    {{ prefixed_columns('speed', [
        'track_id',
        'avg_speed',
        'max_speed',
        'median_speed'
    ]) }}
FROM
    {{ track_speed_stats() }} speed;
