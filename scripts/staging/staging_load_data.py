# staging_load_data.py
import pandas as pd
from sqlalchemy import create_engine
from scripts.logger import logging
from scripts.exceptions import CustomException, error_message_detail
import sys
import os

sys.path.append('/media/moraa/New Volume/Ontita/10Academy/Cohort B/Projects/Week2/Data_Engineering_with_Postgresql_DBT_Airflow')

from scripts.config import (
    csv_file_path,
    staging_postgres_conn
)

class StagingDataLoader:
    def __init__(self, data_filepath: str, postgres_conn_id: str):
        self.data_filepath = data_filepath
        self.postgres_conn_id = postgres_conn_id

    def load_data(self):
        try:
            df_track, df_trajectory = self._read_and_preprocess_csv()

            engine = create_engine(self.postgres_conn_id)

            df_track.to_sql('staging_track_table', engine, index=False, if_exists='replace')
            df_trajectory.to_sql('staging_trajectory_table', engine, index=False, if_exists='replace')

            logging.info("Data loaded into Staging PostgreSQL tables successfully.")
        except Exception as e:
            error_message = "Failed to load data into Staging PostgreSQL tables: {}".format(str(e))
            logging.error(error_message)
            raise CustomException(error_message, error_detail=error_message_detail(e, sys.exc_info()))

    def _read_and_preprocess_csv(self):
        with open(self.data_filepath, 'r') as file:
            lines = file.readlines()

        cols = lines.pop(0).strip('\n').strip().strip(';').split(';')
        track_cols = cols[:4]
        trajectory_cols = ['track_id'] + cols[4:]

        track_info = []
        trajectory_info = []

        for line in lines:
            row = line.strip('\n').strip().strip(';').split(';')
            track_id = row[0]

            track_info.append(row[:4])

            remaining_values = row[4:]
            trajectory_matrix = [[track_id] + remaining_values[i:i+6] for i in range(0, len(remaining_values), 6)]
            trajectory_info.extend(trajectory_matrix)

        df_track = pd.DataFrame(data=track_info, columns=track_cols)
        df_trajectory = pd.DataFrame(data=trajectory_info, columns=trajectory_cols)

        return df_track, df_trajectory

def main():
    try:
        data_loader = StagingDataLoader(csv_file_path, staging_postgres_conn)
        data_loader.load_data()
    except CustomException as ce:
        logging.error("Custom exception occurred: {}".format(ce))
    except Exception as e:
        error_message = "An unexpected error occurred: {}".format(str(e))
        logging.error(error_message)
        raise CustomException(error_message, error_detail=error_message_detail(e, sys.exc_info()))

if __name__ == "__main__":
    main()

