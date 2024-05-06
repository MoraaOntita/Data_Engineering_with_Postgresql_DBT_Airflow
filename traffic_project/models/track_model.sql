-- Create track table
create table {{ ref('df_track') }} (
    track_id bigint,
    type varchar,
    traveled_d numeric,
    avg_speed numeric
);