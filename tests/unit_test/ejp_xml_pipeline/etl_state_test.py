from unittest.mock import patch
from datetime import datetime
import pytest
from botocore.exceptions import ClientError
from ejp_xml_pipeline import etl_state as etl_state_module
from ejp_xml_pipeline.etl_state import (
    update_object_latest_dates,
    get_stored_ejp_xml_processing_state
)
from ejp_xml_pipeline.utils.xml_transform_util.timestamp import (
    convert_datetime_string_to_datetime
)
from ejp_xml_pipeline.dag_pipeline_config.xml_config import eJPXmlDataConfig

EJP_XML_CONFIG = {
    'gcpProjectName': 'elife-data-pipeline',
    'dataPipelineId': 'ejp_xml_pipeline_id',
    'dataset': 'dataset',
    'manuscriptTable': 'sample_manuscript',
    'manuscriptVersionTable': 'sample_manuscript_version',
    'personTable': 'sample_person',
    'personVersion2Table': 'sample_person_v2',
    'eJPXmlBucket': 'ci-elife-data-pipeline',
    'eJPXmlObjectKeyPattern': 'airflow_test/ejp-xml-test-data/ejp_elife_*',
    'stateFile': {
        'bucket': 'state_file_bucket',
        'object': 'state_file_object'
    },
    'tempS3FileStorage': {
        'bucket': 'temp_s3_file_storage_bucket',
        'objectPrefix': 'tmp_s3_storage_obj_prefix'
    }
}


@pytest.fixture(name="mock_download_s3_json_object_exception")
def _download_s3_json_object_with_exception():
    client_error_response = {
        'Error': {
            'Code': 'NoSuchKey'
        }
    }
    with patch.object(
            etl_state_module, 'download_s3_json_object',
            side_effect=ClientError(operation_name='GetObject',
                                    error_response=client_error_response)
    ) as mock:
        yield mock


@pytest.fixture(name="mock_download_s3_json_object")
def _download_s3_json_object_():
    with patch.object(
            etl_state_module, 'download_s3_json_object'
    ) as mock:
        yield mock


class TestUpdateObjectLatestDates:
    def test_should_add_key_if_not_existing_in_existing_state(self):
        existing_state = {
            'key_1_*': datetime.strptime(
                '2020-01-01 01:01', '%Y-%m-%d %H:%M'
            )
        }
        new_key = 'new_key_*'
        new_key_latest_datetime_string = datetime.strptime(
            '2020-02-01 00:00:00', '%Y-%m-%d %H:%M:%S'
        )
        new_state = update_object_latest_dates(
            existing_state, new_key, new_key_latest_datetime_string
        )
        expected_new_state = {
            'key_1_*': '2020-01-01 01:01:00',
            'new_key_*': '2020-02-01 00:00:00'
        }
        assert new_state == expected_new_state

    def test_should_update_key_if_key_in_existing_state(self):
        existing_state = {
            'key_1_*': datetime.strptime(
                '2020-01-01 01:01', '%Y-%m-%d %H:%M'
            )
        }
        new_key = 'key_1_*'
        new_key_latest_datetime_string = datetime.strptime(
            '2020-02-01 00:00:00', '%Y-%m-%d %H:%M:%S'
        )
        new_state = update_object_latest_dates(
            existing_state, new_key, new_key_latest_datetime_string
        )
        expected_new_state = {
            'key_1_*': '2020-02-01 00:00:00'
        }
        assert new_state == expected_new_state

    def test_should_add_key_if_key_existing_state_is_empty(self):
        existing_state = {}
        new_key = 'key_1_*'
        new_key_latest_datetime_string = datetime.strptime(
            '2020-02-01 00:00:00', '%Y-%m-%d %H:%M:%S'
        )
        new_state = update_object_latest_dates(
            existing_state, new_key, new_key_latest_datetime_string
        )
        expected_new_state = {
            'key_1_*': '2020-02-01 00:00:00'
        }
        assert new_state == expected_new_state


class TestGetStoredeJPXmlProcessingState:
    # pylint: disable='unused-argument'
    def test_get_state_no_state_file_in_bucket(
            self, mock_download_s3_json_object_exception
    ):
        ejp_config = eJPXmlDataConfig(EJP_XML_CONFIG, '')
        default_initial_state_timestamp_as_string = (
            "2020-01-01 00:00:00"
        )
        stored_state = get_stored_ejp_xml_processing_state(
            ejp_config,
            default_initial_state_timestamp_as_string
        )
        expected_date = convert_datetime_string_to_datetime(
            default_initial_state_timestamp_as_string
        )
        expected_stored_state = {
            ejp_config.s3_object_key_pattern: expected_date
        }

        assert stored_state == expected_stored_state

    # pylint: disable='unused-argument'
    def test_should_get_state_from_file_in_s3_bucket(
            self, mock_download_s3_json_object
    ):
        ejp_config = eJPXmlDataConfig(EJP_XML_CONFIG, '')
        default_initial_state_timestamp_as_string = ''
        last_stored_modified_timestamp = "2018-01-01 00:00:00"
        mock_download_s3_json_object.return_value = {
            ejp_config.s3_object_key_pattern:
                last_stored_modified_timestamp
        }
        stored_state = get_stored_ejp_xml_processing_state(
            ejp_config,
            default_initial_state_timestamp_as_string
        )
        expected_date = convert_datetime_string_to_datetime(
            last_stored_modified_timestamp
        )
        expected_stored_state = {
            ejp_config.s3_object_key_pattern: expected_date
        }
        assert stored_state == expected_stored_state
