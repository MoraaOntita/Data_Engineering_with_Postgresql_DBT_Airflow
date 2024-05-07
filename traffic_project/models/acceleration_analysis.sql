/*
This model calculates acceleration statistics for each track based on data from the df_trajectory table.
It calculates the average acceleration, maximum acceleration, and median acceleration for each track.
Input: df_trajectory (source table containing trajectory information)
Output: acceleration (view containing aggregated statistics for acceleration analysis)
*/

{{ config(materialized='view') }}

-- Define a macro to add prefix to column names
{% macro prefixed_columns(prefix, columns) -%}
    {%- set prefixed_columns = [] -%}
    {%- for column in columns -%}
        {%- set prefixed_column = prefix ~ '.' ~ column -%}
        {%- do prefixed_columns.append(prefixed_column) -%}
    {%- endfor -%}
    {{- join(prefixed_columns, ', ') -}}
{%- endmacro %}

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
    track_id, -- Identifier for the track
    avg_acceleration, -- Average acceleration of the track
    max_acceleration, -- Maximum acceleration of the track
    median_acceleration -- Median acceleration of the track
FROM
    {{ track_acceleration_stats() }} acceleration;

