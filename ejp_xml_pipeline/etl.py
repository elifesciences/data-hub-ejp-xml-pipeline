from datetime import datetime, timezone
import fnmatch
import os
import io
import logging
from typing import Dict, Iterable, List, Sequence, Tuple
import json

from contextlib import contextmanager
from contextlib import ExitStack
from tempfile import TemporaryDirectory
from pathlib import Path
from zipfile import ZipFile

from typing_extensions import TypedDict

import botocore

from ejp_xml_pipeline.data_store.s3_data_service import (
    s3_open_binary_read,
    download_s3_object_as_string,
    delete_s3_objects,
    upload_file_into_s3
)
from ejp_xml_pipeline.etl_state import (
    get_stored_ejp_xml_processing_state,
    update_object_latest_dates,
    update_state
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


LOGGER = logging.getLogger(__name__)


class FileMetadata(TypedDict):
    name: str
    last_modified: datetime


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
                open(ent_conf.get_full_file_location(), "w", encoding="UTF-8")
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


# pylint: disable=too-many-arguments, too-many-positional-arguments
def download_load2bq_cleanup_temp_files(
        matching_file_metadata_iter: Iterable[Tuple[FileMetadata, str]],
        s3_bucket: str,
        gcp_project: str,
        dataset: str,
        bq_table: str,
        batch_size_limit: int = 100000
):
    written_file_row_count = 0
    s3_objects_written_to_file = []
    with TemporaryDirectory() as tmp_dir:
        temp_file_name = str(
            Path(tmp_dir, "downloaded_file")
        )
        with open(temp_file_name, 'a', encoding="UTF-8") as writer:
            for matching_file_metadata, _ in matching_file_metadata_iter:
                s3_object = matching_file_metadata['name']
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


def etl_new_ejp_xml_files(
    data_config: eJPXmlDataConfig,
    default_initial_s3_last_modified_date_str: str
) -> None:
    obj_pattern_with_latest_dates = (
        get_stored_ejp_xml_processing_state(
            data_config,
            default_initial_s3_last_modified_date_str
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


def load_temp_ejp_json_files_to_bq(
    data_config: eJPXmlDataConfig
) -> None:
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
