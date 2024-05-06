
{% macro calculate_traveled_distance() %}
with distance_calc as (
    select
        track_id,
        sum(distance) as traveled_distance
    from (
        select
            track_id,
            -- Calculating distance traveled using haversine formula
            haversine(lon, lat, lag(lon) over (partition by track_id order by time), lag(lat) over (partition by track_id order by time)) as distance
        from
            {{ ref('swarm_uavs.df_track') }}
    ) sub
    group by
        track_id
)
select * from distance_calc
{% endmacro %}
