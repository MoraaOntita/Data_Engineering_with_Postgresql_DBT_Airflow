# create_tables.py
from db_connections import connect_to_database

def create_track_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS track (
            track_id SERIAL PRIMARY KEY,
            type VARCHAR(100) NOT NULL,
            traveled_d FLOAT NOT NULL CHECK (traveled_d >= 0),
            avg_speed FLOAT NOT NULL CHECK (avg_speed >= 0)
        );
    """)
    conn.commit()
    cur.close()

def create_trajectory_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trajectory (
            trajectory_id SERIAL PRIMARY KEY,
            track_id INTEGER NOT NULL REFERENCES track(track_id),
            lat FLOAT NOT NULL,
            lon FLOAT NOT NULL,
            speed FLOAT NOT NULL CHECK (speed >= 0),
            lon_acc FLOAT NOT NULL,
            lat_acc FLOAT NOT NULL,
            time FLOAT NOT NULL CHECK (time >= 0)
        );
    """)
    conn.commit()
    cur.close()
