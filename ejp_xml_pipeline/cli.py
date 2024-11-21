import logging
import os
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Sequence, Tuple
import fnmatch

import botocore.session
import yaml

from ejp_xml_pipeline.dag_pipeline_config.xml_config import eJPXmlDataConfig
from ejp_xml_pipeline.etl_state import get_stored_ejp_xml_processing_state
from ejp_xml_pipeline.etl import (
    FileMetadata,
    etl_ejp_xml_zip,
    download_load2bq_cleanup_temp_files
)
from ejp_xml_pipeline.etl_state import (
    update_state,
    update_object_latest_dates,
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


def get_s3_client():
    session = botocore.session.get_session()
    return session.create_client('s3')


def list_objects_with_pattern_and_timestamp(
    s3_client,
    bucket: str,
    pattern: str,
    latest_timestamp: datetime
) -> Sequence[FileMetadata]:
    """
    List objects in S3 matching pattern and modified after latest_timestamp
    """
    prefix = pattern.split('*')[0]
    paginator = s3_client.get_paginator('list_objects_v2')
    matching_objects: List[FileMetadata] = []
    LOGGER.info(
        'listing s3 objects with bucket: %s, pattern: %s and prefix: %s',
        bucket,
        pattern,
        prefix
    )
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    last_modified = obj['LastModified']
                    # Check both pattern match and timestamp
                    if (fnmatch.fnmatch(key, pattern) and last_modified > latest_timestamp):
                        matching_objects.append({
                            'name': key,
                            'last_modified': last_modified
                        })
    except botocore.exceptions.ClientError as err:
        LOGGER.error('Error listing objects with prefix %s: %s', prefix, err)
        raise

    return matching_objects


def etl_s3_object_pattern(
        data_config: 'eJPXmlDataConfig',
        obj_pattern_with_latest_dates: dict,
        s3_bucket_name: str
) -> Iterable[Tuple[FileMetadata, str]]:

    s3_client = get_s3_client()
    matching_files: Dict[str, Sequence[FileMetadata]] = {}

    # For each pattern and its timestamp, get matching objects
    for pattern, latest_timestamp in obj_pattern_with_latest_dates.items():
        objects = list_objects_with_pattern_and_timestamp(
            s3_client,
            s3_bucket_name,
            pattern,
            latest_timestamp
        )
        if objects:
            matching_files[pattern] = objects
            LOGGER.info(
                'Found %d new files for pattern %s modified after %s',
                len(objects),
                pattern,
                latest_timestamp.isoformat()
            )

    # Process matching files
    for object_key_pattern, files_list in matching_files.items():
        sorted_files = sorted(
            files_list,
            key=lambda x: x['last_modified']
        )

        for object_index, file_metadata in enumerate(sorted_files):
            object_key = file_metadata['name']
            s3_bucket = (
                data_config.temp_file_s3_bucket
                if object_key.endswith('.json')
                else data_config.s3_bucket
            )

            LOGGER.info(
                'processing file (%d / %d): s3://%s/%s',
                1 + object_index,
                len(sorted_files),
                s3_bucket,
                object_key
            )

            yield file_metadata, object_key_pattern


def etl_new_ejp_xml_files() -> None:
    data_config = get_config()
    obj_pattern_with_latest_dates = (
        get_stored_ejp_xml_processing_state(
            data_config,
            get_default_initial_s3_last_modified_date()
        )
    )
    LOGGER.info('obj_pattern_with_latest_dates: %s', obj_pattern_with_latest_dates)
    matching_file_metadata_iter = etl_s3_object_pattern(
        data_config,
        obj_pattern_with_latest_dates,
        data_config.s3_bucket,
    )

    for matching_file_metadata, object_key_pattern in matching_file_metadata_iter:
        LOGGER.info('matching_file_metadata: %s', matching_file_metadata)
        object_key = matching_file_metadata['name']

        etl_ejp_xml_zip(
            data_config, object_key,
        )

        updated_obj_pattern_with_latest_dates = (
            update_object_latest_dates(
                obj_pattern_with_latest_dates,
                object_key_pattern,
                matching_file_metadata['last_modified']
            )
        )
        update_state(
            updated_obj_pattern_with_latest_dates,
            data_config.state_file_bucket,
            data_config.state_file_object
        )


def load_temp_ejp_json_files_to_bq() -> None:
    data_config = get_config()
    batch_size_limit = 100000
    for entity_type in data_config.entity_type_mapping.values():
        obj_pattern_with_latest_date = {
            entity_type.s3_object_wildcard_prefix:
                datetime.min.replace(tzinfo=timezone.min)
        }
        matching_file_metadata_iter = etl_s3_object_pattern(
            data_config,
            obj_pattern_with_latest_date,
            data_config.temp_file_s3_bucket,
        )
        download_load2bq_cleanup_temp_files(
            matching_file_metadata_iter,
            data_config.temp_file_s3_bucket,
            data_config.gcp_project,
            data_config.dataset,
            entity_type.table_name,
            batch_size_limit
        )


def main():
    etl_new_ejp_xml_files()
    load_temp_ejp_json_files_to_bq()
    LOGGER.info('Data fetch and load process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
