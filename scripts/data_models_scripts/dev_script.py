import pandas as pd
import os
import sys

sys.path.append('/media/moraa/New Volume/Ontita/10Academy/Cohort B/Projects/Week2/Data_Engineering_with_Postgresql_DBT_Airflow')

from sqlalchemy import create_engine, inspect  # Importing inspect
from datetime import datetime
from scripts.logger import logging
from scripts.exceptions import CustomException, error_message_detail
from scripts.utils import read_csv_file, modify_csv_data
from scripts.data_models_scripts.db_connections import create_db_engine

sys.path.append("/media/moraa/New Volume/Ontita/10Academy/Cohort B/Projects/Week2/Data_Engineering_with_Postgresql_DBT_Airflow")

def setup_logger():
    """Sets up the logger configuration."""
    LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
    logs_path = os.path.join(os.getcwd(),"logs", LOG_FILE)
    os.makedirs(logs_path, exist_ok=True)
    LOG_FILE_PATH = os.path.join(logs_path, LOG_FILE)
    logging.basicConfig(
        filename=LOG_FILE_PATH,
        format="[%(asctime)s] %(lineno)d %(name)s -%(levelname)s - %(message)s",
        level=logging.INFO
    )

def create_tables(engine, df_track, df_trajectory):
    """Creates tables in PostgreSQL if they don't exist and loads data."""
    track_table_name = 'track_table'
    trajectory_table_name = 'trajectory_table'

    with engine.connect() as conn:
        # Create track table
        if not engine.dialect.has_table(conn, track_table_name):
            conn.execute(f"""
                CREATE TABLE {track_table_name} (
                    -- Define your columns here based on the structure of df_track
                );
            """)
            df_track.to_sql(track_table_name, conn, if_exists='append', index=False)

        # Create trajectory table
        if not engine.dialect.has_table(conn, trajectory_table_name):
            conn.execute(f"""
                CREATE TABLE {trajectory_table_name} (
                    -- Define your columns here based on the structure of df_trajectory
                );
            """)
            df_trajectory.to_sql(trajectory_table_name, conn, if_exists='append', index=False)


def main():
    try:
        # Setup logger
        setup_logger()

        # Define CSV file path
        csv_file_path = "/media/moraa/New Volume/Ontita/10Academy/Cohort B/Projects/Week2/Data_Engineering_with_Postgresql_DBT_Airflow/Data/20181024_d1_0830_0900.csv"

        # Read CSV file and modify data
        lines = read_csv_file(csv_file_path)
        modified_data = modify_csv_data(lines)

        # Extract track and trajectory information
        track_info = []
        trajectory_info = []
        for row in modified_data:
            track_id = row[0]
            track_info.append(row[:4])
            remaining_values = row[4:]
            trajectory_matrix = [[track_id] + remaining_values[i:i+6] for i in range(0, len(remaining_values), 6)]
            trajectory_info = trajectory_info + trajectory_matrix

        # Create DataFrames
        track_cols = ['track_id', 'type', 'traveled_d', 'avg_speed']
        trajectory_cols = ['track_id', 'lat', 'lon', 'speed', 'lon_acc', 'lat_acc', 'time']
        df_track = pd.DataFrame(data=track_info, columns=track_cols)
        df_trajectory = pd.DataFrame(data=trajectory_info, columns=trajectory_cols)

        # Create engine using the function from db_connections
        engine = create_db_engine()

        # Create tables in PostgreSQL and load data
        create_tables(engine, df_track, df_trajectory)

        logging.info("Tables created and data loaded into PostgreSQL successfully.")
    except Exception as e:
        error_message = str(e)
        error_detail = sys.exc_info()
        raise CustomException(error_message, error_detail)

if __name__ == "__main__":
    main()


