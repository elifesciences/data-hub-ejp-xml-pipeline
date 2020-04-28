import os
import logging
import json
import io

import yaml
from contextlib import contextmanager
from contextlib import ExitStack
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from data_pipeline.data_store.s3_data_service import s3_open_binary_read
from data_pipeline.transform_zip_xml.ejp_zip import (
    iter_parse_xml_in_zip,
)
from data_pipeline.dag_pipeline_config.xml_config import eJPXmlDataConfig
from data_pipeline.data_store.bq_data_service import load_file_into_bq, create_or_extend_table_schema

LOGGER = logging.getLogger(__name__)


def write_entities_in_parsed_doc(
        parsed_document_entities,
        opened_file_for_entity_type
):

    for entity in parsed_document_entities:
        writer = opened_file_for_entity_type.get(
            type(entity)
        )
        writer.write(json.dumps(
            remove_key_with_null_value(entity.data)
        ))
        writer.write("\n")


def load_entities_file_to_bq(ejp_xml_load_config: eJPXmlDataConfig):
    for entity in ejp_xml_load_config.entity_type_mapping.values():
        if os.path.getsize(entity.get_full_file_location()) > 0:
            load_data_to_bq(
                temp_processed_jsonl_path=entity.get_full_file_location(),
                dataset_name=ejp_xml_load_config.dataset,
                table_name=entity.table_name,
                gcp_project=ejp_xml_load_config.gcp_project
            )


def load_data_to_bq(
        gcp_project,
        dataset_name,
        table_name,
        temp_processed_jsonl_path,
):
    create_or_extend_table_schema(
        gcp_project,
        dataset_name,
        table_name,
        temp_processed_jsonl_path
    )

    load_file_into_bq(
        filename=temp_processed_jsonl_path,
        table_name=table_name,
        dataset_name=dataset_name,
        project_name=gcp_project
    )


@contextmanager
def get_temp_opened_file_for_entity_types(
        ejp_xml_data_config: eJPXmlDataConfig,
):
    with ExitStack() as stack, TemporaryDirectory() as tempdir:
        for ent_db_load_config in ejp_xml_data_config.entity_type_mapping.values():
            ent_db_load_config.set_directory(tempdir)
        opened_files = {
            ent_type: stack.enter_context(
                open(ent_conf.get_full_file_location(), "w")
            )
            for ent_type, ent_conf in ejp_xml_data_config.entity_type_mapping.items()
        }
        yield opened_files


def transform_load_data(
        ejp_xml_data_config: eJPXmlDataConfig, object_key: str
):
    with get_temp_opened_file_for_entity_types(ejp_xml_data_config) as temp_opened_file_for_entity_type:
        with s3_open_binary_read(
                bucket=ejp_xml_data_config.s3_bucket,
                object_key=object_key
        ) as streaming_body:

            with io.BytesIO(streaming_body.read()) as tf:
                tf.seek(0)
                with ZipFile(tf, mode='r') as zip_file:
                    parsed_documents = (
                        iter_parse_xml_in_zip(
                            zip_file,
                            zip_filename=object_key,
                        )
                    )
                    for parsed_document in parsed_documents:
                        write_entities_in_parsed_doc(
                            parsed_document.get_entities(),
                            temp_opened_file_for_entity_type
                        )
        load_entities_file_to_bq(ejp_xml_data_config)


def remove_key_with_null_value(record):
    if isinstance(record, dict):
        for key in list(record):
            val = record.get(key)
            if not val:
                record.pop(key, None)
            elif isinstance(val, (dict, list)):
                remove_key_with_null_value(val)

    elif isinstance(record, list):
        for index, val in enumerate(record):
            if isinstance(val, (dict, list)):
                remove_key_with_null_value(val)
            else:
                if val:
                    record[index] = val
    return record


class NamedLiterals:
    DAG_RUN = 'dag_run'
    RUN_ID = 'run_id'
    DAG_RUNNING_STATUS = 'running'
    S3_FILE_METADATA_NAME_KEY = "Key"
    S3_FILE_METADATA_LAST_MODIFIED_KEY = "LastModified"
    DEFAULT_AWS_CONN_ID = "aws_default"
    EJP_XML_CONFIG_FILE_PATH_ENV_NAME = "EJP_XML_CONFIG_FILE_PATH"


def get_yaml_file_as_dict(file_location: str) -> dict:
    with open(file_location, 'r') as yaml_file:
        return yaml.safe_load(yaml_file)
