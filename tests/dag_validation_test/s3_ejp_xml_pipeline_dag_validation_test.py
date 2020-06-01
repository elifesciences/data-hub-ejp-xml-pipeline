import pytest

from dags.s3_xml_import_pipeline import (
    DAG_ID
)
from tests.dag_validation_test import (
    dag_should_contain_named_tasks
)


def test_target_dag_should_contain_four_tasks(dagbag):
    target_dag = dagbag.get_dag(DAG_ID)
    assert len(target_dag.tasks) == 5


@pytest.mark.parametrize(
    "dag_id, task_list",
    [
        (DAG_ID,
         ['Should_Remaining_Tasks_Execute',
          'Update_Previous_RunID_Variable_Value_For_DagRun_Locking',
          'S3_Key_Sensor_Task',
          'ETL_eJP_XML_To_S3_JSON',
          'Load_S3_JSON_To_BQ'
          ])
    ],
)
def test_dag_should_contain_named_tasks(dagbag, dag_id, task_list):
    dag_should_contain_named_tasks(dagbag, dag_id, task_list)
