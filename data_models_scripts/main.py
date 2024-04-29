# main.py
from db_connections import connect_to_database
from create_tables import create_track_table, create_trajectory_table

def main():
    # Connect to the database
    conn = connect_to_database()

    # Create track table
    create_track_table(conn)

    # Create trajectory table
    create_trajectory_table(conn)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()
