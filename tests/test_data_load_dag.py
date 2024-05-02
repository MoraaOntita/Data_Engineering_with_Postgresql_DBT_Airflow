import sys
sys.path.append('/media/moraa/New Volume/Ontita/10Academy/Cohort B/Projects/Week2/Data_Engineering_with_Postgresql_DBT_Airflow')

import unittest
from airflow.models import DagBag
from datetime import datetime
from dags.load_data_dag import (
    load_data_to_prod,
    load_data_to_dev,
    load_data_to_staging
)

class TestDataLoadDag(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(include_examples=False)  # Load the DAGs without example DAGs

    def test_task_count(self):
        """Check the number of tasks in the DAG"""
        dag_id = 'data_load_dag'
        dag = self.dagbag.get_dag(dag_id)
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 3)  # Assuming you have three tasks

    def test_task_dependencies(self):
        """Check the task dependencies in the DAG"""
        dag_id = 'data_load_dag'
        dag = self.dagbag.get_dag(dag_id)
        self.assertIsNotNone(dag)

        # Check task dependencies
        task_dependencies = dag.get_task_instances(datetime.now())
        expected_dependencies = {
            'load_data_to_prod': ['load_data_to_dev', 'load_data_to_staging']
        }
        for task_id, upstream_task_ids in expected_dependencies.items():
            task = dag.get_task(task_id)
            upstream_tasks = task.get_direct_relatives(upstream=True)
            upstream_task_ids_actual = [t.task_id for t in upstream_tasks]
            self.assertCountEqual(upstream_task_ids_actual, upstream_task_ids)

if __name__ == '__main__':
    unittest.main()

