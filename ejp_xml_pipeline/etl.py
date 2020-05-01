import os
import io
import logging
import json

from contextlib import contextmanager
from contextlib import ExitStack
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from ejp_xml_pipeline.data_store.s3_data_service import s3_open_binary_read
from ejp_xml_pipeline.transform_zip_xml.ejp_zip import (
    iter_parse_xml_in_zip,
)
from ejp_xml_pipeline.transform_json import remove_key_with_null_value
from ejp_xml_pipeline.dag_pipeline_config.xml_config import eJPXmlDataConfig
from ejp_xml_pipeline.data_store.bq_data_service import (
    load_file_into_bq, create_or_extend_table_schema
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
):
    with ExitStack() as stack, TemporaryDirectory() as tempdir:
        for ent_db_load_config \
                in ejp_xml_data_config.entity_type_mapping.values():
            ent_db_load_config.set_directory(tempdir)
        opened_files = {
            ent_type: stack.enter_context(
                open(ent_conf.get_full_file_location(), "w")
            )
            for ent_type, ent_conf
            in ejp_xml_data_config.entity_type_mapping.items()
        }
        yield opened_files


def etl_ejp_xml_zip(
        ejp_xml_data_config: eJPXmlDataConfig, object_key: str
):
    with get_opened_temp_file_for_entity_types(
            ejp_xml_data_config
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
                        )
                    )
                    for parsed_document in parsed_documents:
                        write_entities_in_parsed_doc_to_file(
                            parsed_document.get_entities(),
                            temp_opened_file_for_entity_type
                        )
        load_entities_file_to_bq(ejp_xml_data_config)


def load_entities_file_to_bq(ejp_xml_load_config: eJPXmlDataConfig):
    for entity in ejp_xml_load_config.entity_type_mapping.values():
        if os.path.getsize(entity.get_full_file_location()) > 0:
            create_or_extend_table_schema(
                ejp_xml_load_config.gcp_project,
                ejp_xml_load_config.dataset,
                entity.table_name,
                entity.get_full_file_location()
            )

            load_file_into_bq(
                filename=entity.get_full_file_location(),
                table_name=entity.table_name,
                dataset_name=ejp_xml_load_config.dataset,
                project_name=ejp_xml_load_config.gcp_project
            )
