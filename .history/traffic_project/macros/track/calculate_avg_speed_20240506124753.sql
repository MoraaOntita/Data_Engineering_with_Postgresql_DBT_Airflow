
{% macro calculate_avg_speed() %}
with speed_calc as (
    select
        track_id,
        avg(speed) as avg_speed
    from
        {{ ref('swarm_uavs.df_track') }}
    group by
        track_id
)
select * from speed_calc
{% endmacro %}
