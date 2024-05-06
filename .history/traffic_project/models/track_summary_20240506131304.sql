-- models/track_summary.sql
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
    {{ prefixed_columns('summary', [
        'track_id',
        'type',
        'total_distance',
        'avg_speed',
        'max_speed'
    ]) }}
FROM
    {{ track_summary_summary_stats() }} summary;

{% test not_null('traveled_d') %}
{% test unique('track_id') %}