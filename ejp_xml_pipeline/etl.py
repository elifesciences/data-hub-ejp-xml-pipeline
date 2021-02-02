import os
import io
import logging
from typing import List
import json

from contextlib import contextmanager
from contextlib import ExitStack
from tempfile import TemporaryDirectory
from pathlib import Path
from zipfile import ZipFile

from ejp_xml_pipeline.data_store.s3_data_service import (
    s3_open_binary_read,
    download_s3_object_as_string,
    delete_s3_objects,
    upload_file_into_s3
)
from ejp_xml_pipeline.transform_zip_xml.ejp_zip import (
    iter_parse_xml_in_zip,
)
from ejp_xml_pipeline.transform_json import remove_key_with_null_value
from ejp_xml_pipeline.dag_pipeline_config.xml_config import (
    eJPXmlDataConfig
)
from ejp_xml_pipeline.data_store.bq_data_service import (
    load_file_into_bq, create_or_extend_table_schema
)
from ejp_xml_pipeline.utils import (
    NamedDataPipelineLiterals as named_literals,
)


LOGGER = logging.getLogger(__name__)


def write_entities_in_parsed_doc_to_file(
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


@contextmanager
def get_opened_temp_file_for_entity_types(
        ejp_xml_data_config: eJPXmlDataConfig,
        file_dir: str
):
    with ExitStack() as stack:
        for ent_db_load_config \
                in ejp_xml_data_config.entity_type_mapping.values():
            ent_db_load_config.set_directory(file_dir)
        opened_files = {
            ent_type: stack.enter_context(
                open(ent_conf.get_full_file_location(), "w")
            )
            for ent_type, ent_conf
            in ejp_xml_data_config.entity_type_mapping.items()
        }
        yield opened_files


def etl_ejp_xml_zip(
        ejp_xml_data_config: eJPXmlDataConfig, object_key: str,
):
    with TemporaryDirectory() as file_dir:
        with get_opened_temp_file_for_entity_types(
                ejp_xml_data_config, file_dir
        ) as temp_opened_file_for_entity_type:
            with s3_open_binary_read(
                    bucket=ejp_xml_data_config.s3_bucket,
                    object_key=object_key
            ) as streaming_body:
                with io.BytesIO(streaming_body.read()) as zip_buffer:
                    zip_buffer.seek(0)
                    with ZipFile(zip_buffer, mode='r') as zip_file:
                        parsed_documents = (
                            iter_parse_xml_in_zip(
                                zip_file,
                                zip_filename=object_key,
                                xml_filename_exclusion_regex_pattern=(
                                    ejp_xml_data_config.xml_filename_exclusion_regex_pattern
                                )
                            )
                        )
                        for parsed_document in parsed_documents:
                            write_entities_in_parsed_doc_to_file(
                                parsed_document.get_entities(),
                                temp_opened_file_for_entity_type
                            )
        load_entities_file_to_s3(
            ejp_xml_data_config,
            object_key
        )


def get_temp_s3_object_name(
        obj_prefix_in_config: str,
        original_object_name: str
):
    obj_name = obj_prefix_in_config + original_object_name + '.json'

    return obj_name


def load_entities_file_to_s3(
        ejp_xml_load_config: eJPXmlDataConfig,
        original_obj_key
):
    for entity in ejp_xml_load_config.entity_type_mapping.values():
        if os.path.getsize(entity.get_full_file_location()) > 0:
            obj_key = get_temp_s3_object_name(
                entity.s3_object_prefix,
                original_obj_key
            )
            upload_file_into_s3(
                bucket=ejp_xml_load_config.temp_file_s3_bucket,
                object_key=obj_key,
                full_file_path=entity.get_full_file_location()
            )


def load_entity_file_to_bq(
        gcp_project: str,
        dataset: str,
        table_name: str,
        file_path: str
):
    if os.path.getsize(file_path) > 0:
        create_or_extend_table_schema(
            gcp_project,
            dataset,
            table_name,
            file_path
        )
        load_file_into_bq(
            filename=file_path,
            table_name=table_name,
            dataset_name=dataset,
            project_name=gcp_project
        )


# pylint: disable='too-many-arguments'
def download_load2bq_cleanup_temp_files(
        matching_file_metadata_iter, s3_bucket: str,
        gcp_project: str, dataset: str,
        bq_table: str, batch_size_limit: int = 100000
):
    written_file_row_count = 0
    s3_objects_written_to_file = []
    with TemporaryDirectory() as tmp_dir:
        temp_file_name = str(
            Path(tmp_dir, "downloaded_file")
        )
        with open(temp_file_name, 'a') as writer:
            for matching_file_metadata, _ in matching_file_metadata_iter:
                s3_object = matching_file_metadata.get(
                    named_literals.S3_FILE_METADATA_NAME_KEY
                )
                jsonl_string = download_s3_object_as_string(
                    s3_bucket,
                    s3_object
                )
                writer.write(jsonl_string)
                written_file_row_count += (
                    get_number_of_lines(jsonl_string)
                )
                s3_objects_written_to_file.append(
                    s3_object
                )
                if written_file_row_count > batch_size_limit:
                    writer.flush()
                    load_and_delete_temp_objects(
                        gcp_project, dataset,
                        bq_table, temp_file_name,
                        s3_bucket, s3_objects_written_to_file
                    )
                    writer.truncate()
                    s3_objects_written_to_file = []
                    written_file_row_count = 0
            writer.flush()
            load_and_delete_temp_objects(
                gcp_project, dataset,
                bq_table, temp_file_name,
                s3_bucket, s3_objects_written_to_file
            )


def get_number_of_lines(jsonl_string: str):
    return len(jsonl_string.splitlines())


def load_and_delete_temp_objects(
        gcp_project: str, dataset: str,
        bq_table: str, tempfile_name: str,
        s3_bucket: str, s3_objects_written_to_file: List[str]
):
    load_entity_file_to_bq(
        gcp_project, dataset,
        bq_table, tempfile_name
    )
    delete_s3_objects(
        s3_bucket, s3_objects_written_to_file
    )
