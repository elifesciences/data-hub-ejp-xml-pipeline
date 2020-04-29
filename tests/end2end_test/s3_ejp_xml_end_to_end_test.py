import os
import logging

from data_pipeline.utils import get_yaml_file_as_dict
from data_pipeline.dag_pipeline_config.xml_config import (
    eJPXmlDataConfig
)
from dags.s3_xml_import_pipeline import (
    DAG_ID,
    named_literals
)
from dags.s3_xml_import_pipeline import (
    DEFAULT_DEPLOYMENT_ENV_VALUE,
    DEPLOYMENT_ENV_ENV_NAME
)
from tests.end2end_test import (
    trigger_run_test_pipeline,
    DataPipelineCloudResource
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI
)


LOGGER = logging.getLogger(__name__)


def test_dag_runs_data_imported():
    airflow_api = AirflowAPI()

    data_pipeline_cloud_resource = (
        get_data_pipeline_cloud_resource()
    )
    trigger_run_test_pipeline(
        airflow_api,
        DAG_ID,
        data_pipeline_cloud_resource
    )


def get_data_pipeline_cloud_resource():
    conf_file_path = os.getenv(
        named_literals.EJP_XML_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV_VALUE
    )
    ejp_xml_etl_config = eJPXmlDataConfig(
        data_config_dict, dep_env
    )

    return DataPipelineCloudResource(
        ejp_xml_etl_config.gcp_project,
        ejp_xml_etl_config.dataset,
        ejp_xml_etl_config.manuscript_table,
        ejp_xml_etl_config.manuscript_version_table,
        ejp_xml_etl_config.person_table,
        ejp_xml_etl_config.person_v2_table,
        ejp_xml_etl_config.state_file_bucket,
        ejp_xml_etl_config.state_file_object
    )
