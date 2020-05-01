import time
import logging
from tests.end2end_test.end_to_end_test_helper import (
    simple_query
)
from ejp_xml_pipeline.data_store.s3_data_service import (
    delete_s3_object
)
from ejp_xml_pipeline.dag_pipeline_config.xml_config import eJPXmlDataConfig
LOGGER = logging.getLogger(__name__)


# pylint: disable=broad-except
def truncate_table(
        project_name: str,
        dataset_name: str,
        table_name: str
):
    try:
        simple_query(
            query=TestQueryTemplate.CLEAN_TABLE_QUERY,
            project=project_name,
            dataset=dataset_name,
            table=table_name,
        )
    except Exception:
        LOGGER.info("table not cleaned, maybe it does not exist")


def delete_statefile_if_exist(
        state_file_bucket_name,
        state_file_object_name
):

    try:
        delete_s3_object(state_file_bucket_name,
                         state_file_object_name
                         )
    except Exception:
        LOGGER.info("s3 object not deleted, may not exist")


def get_table_row_count(
        project_name,
        dataset_name,
        table_name
):
    query_response = simple_query(
        query=TestQueryTemplate.READ_COUNT_TABLE_QUERY,
        project=project_name,
        dataset=dataset_name,
        table=table_name,
    )
    return query_response[0].get("count")


# pylint: disable=too-many-arguments
def trigger_run_test_pipeline(
        airflow_api, dag_id,
        ejp_xml_config: eJPXmlDataConfig
):
    for table in [
            ejp_xml_config.manuscript_table,
            ejp_xml_config.manuscript_version_table,
            ejp_xml_config.person_table,
            ejp_xml_config.person_v2_table
    ]:
        truncate_table(
            ejp_xml_config.gcp_project,
            ejp_xml_config.dataset,
            table,
        )
    delete_statefile_if_exist(
        ejp_xml_config.state_file_bucket,
        ejp_xml_config.state_file_object
    )
    airflow_api.unpause_dag(dag_id)
    execution_date = airflow_api.trigger_dag(dag_id=dag_id)
    is_dag_running = wait_till_triggered_dag_run_ends(
        dag_id, execution_date, airflow_api
    )
    assert not is_dag_running
    assert airflow_api.get_dag_status(dag_id, execution_date) == "success"
    for table in [
            ejp_xml_config.manuscript_table,
            ejp_xml_config.manuscript_version_table,
            ejp_xml_config.person_table,
            ejp_xml_config.person_v2_table
    ]:
        loaded_table_row_count = get_table_row_count(
            ejp_xml_config.gcp_project,
            ejp_xml_config.dataset,
            table
        )
        assert loaded_table_row_count > 0


def wait_till_triggered_dag_run_ends(
        dag_id,
        execution_date, airflow_api
):
    is_dag_running = True
    while is_dag_running:
        is_dag_running = airflow_api.is_dag_running(dag_id, execution_date)
        LOGGER.info("etl in progress")
        time.sleep(5)
    time.sleep(10)
    return is_dag_running


class TestQueryTemplate:
    CLEAN_TABLE_QUERY = """
    DELETE FROM `{project}.{dataset}.{table}` WHERE true
    """
    READ_COUNT_TABLE_QUERY = """
    SELECT COUNT(*) AS count FROM `{project}.{dataset}.{table}`
    """
