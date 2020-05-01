import os
import logging

from ejp_xml_pipeline.utils import get_yaml_file_as_dict
from ejp_xml_pipeline.dag_pipeline_config.xml_config import (
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
    trigger_run_test_pipeline
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI
)


LOGGER = logging.getLogger(__name__)


def test_dag_runs_data_imported():
    airflow_api = AirflowAPI()
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

    trigger_run_test_pipeline(
        airflow_api,
        DAG_ID,
        ejp_xml_etl_config
    )
