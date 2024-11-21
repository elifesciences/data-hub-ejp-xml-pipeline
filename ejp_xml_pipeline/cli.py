import logging
import os

import yaml

from ejp_xml_pipeline.dag_pipeline_config.xml_config import eJPXmlDataConfig
from ejp_xml_pipeline.etl import (
    etl_new_ejp_xml_files,
    load_temp_ejp_json_files_to_bq
)


LOGGER = logging.getLogger(__name__)


EJP_XML_CONFIG_FILE_PATH_ENV_NAME = (
    "EJP_XML_CONFIG_FILE_PATH"
)


INITIAL_S3_XML_FILE_LAST_MODIFIED_DATE_ENV_NAME = (
    "INITIAL_S3_XML_FILE_LAST_MODIFIED_DATE"
)
S3_BUCKET_POLLING_INTERVAL_IN_MINUTES_ENV_VAR_NAME = (
    "S3_EJP_XML_BUCKET_POLLING_INTERVAL_IN_MINUTES"
)
S3_BUCKET_POLLING_TIMEOUT_IN_MINUTES_ENV_VAR_NAME = (
    "S3_EJP_XML_BUCKET_POLLING_TIMEOUT_IN_MINUTES"
)

DEFAULT_INITIAL_S3_XML_FILE_LAST_MODIFIED_DATE = "2020-01-01 00:00:00"

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"


def get_yaml_file_as_dict(file_location: str) -> dict:
    with open(file_location, 'r', encoding="UTF-8") as yaml_file:
        return yaml.safe_load(yaml_file)


def get_default_initial_s3_last_modified_date():
    return os.getenv(
        INITIAL_S3_XML_FILE_LAST_MODIFIED_DATE_ENV_NAME,
        DEFAULT_INITIAL_S3_XML_FILE_LAST_MODIFIED_DATE
    )


def get_config() -> eJPXmlDataConfig:
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV_VALUE
    )
    conf_file_path = os.environ[EJP_XML_CONFIG_FILE_PATH_ENV_NAME]
    LOGGER.info('conf_file_path: %s', conf_file_path)
    data_config_dict = get_yaml_file_as_dict(
        conf_file_path
    )
    LOGGER.info('data_config_dict: %s', data_config_dict)
    return eJPXmlDataConfig(data_config_dict, dep_env)


def main():
    data_config = get_config()
    default_initial_s3_last_modified_date_str = get_default_initial_s3_last_modified_date()
    etl_new_ejp_xml_files(
        data_config=data_config,
        default_initial_s3_last_modified_date_str=default_initial_s3_last_modified_date_str
    )
    load_temp_ejp_json_files_to_bq(
        data_config=data_config
    )
    LOGGER.info('Data fetch and load process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
