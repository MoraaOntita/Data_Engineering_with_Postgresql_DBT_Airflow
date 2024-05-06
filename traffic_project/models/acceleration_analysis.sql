-- models/acceleration_analysis.sql
{{ config(materialized='view') }}

-- Define a CTE to calculate acceleration statistics for each track
WITH track_acceleration_stats AS (
    SELECT
        track_id,
        AVG(acceleration) AS avg_acceleration,
        MAX(acceleration) AS max_acceleration,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY acceleration) AS median_acceleration
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
{% macro track_acceleration_stats() -%}
    SELECT
        track_id,
        avg_acceleration,
        max_acceleration,
        median_acceleration
    FROM
        track_acceleration_stats
{%- endmacro %}

-- Define the main model
SELECT
    {{ prefixed_columns('acceleration', [
        'track_id',
        'avg_acceleration',
        'max_acceleration',
        'median_acceleration'
    ]) }}
FROM
    {{ track_acceleration_stats() }} acceleration;
