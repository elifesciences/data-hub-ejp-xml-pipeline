import json

from botocore.exceptions import ClientError
from ejp_xml_pipeline.dag_pipeline_config.xml_config import eJPXmlDataConfig
from ejp_xml_pipeline.data_store.s3_data_service import (
    download_s3_json_object, upload_s3_object
)
from ejp_xml_pipeline.utils.xml_transform_util.timestamp import (
    convert_datetime_string_to_datetime, convert_datetime_to_string
)


def update_state(
        s3_obj_pattern_with_latest_dates: dict,
        statefile_s3_bucket: str,
        statefile_s3_object: str
):
    upload_s3_object(
        bucket=statefile_s3_bucket,
        object_key=statefile_s3_object,
        data_object=json.dumps(s3_obj_pattern_with_latest_dates)
    )


def get_stored_state(
        data_config: eJPXmlDataConfig,
        default_latest_file_date,
):
    try:
        downloaded_state = download_s3_json_object(
            data_config.state_file_bucket,
            data_config.state_file_object
        )
        stored_state = {
            object_pattern:
                downloaded_state.get(
                    object_pattern, default_latest_file_date
                )
            for object_pattern in [data_config.s3_object_key_pattern]
        }
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            stored_state = get_initial_state(data_config,
                                             default_latest_file_date
                                             )
        else:
            raise ex
    return {
        k: convert_datetime_string_to_datetime(v)
        for k, v in stored_state.items()
    }


def get_initial_state(
        data_config: eJPXmlDataConfig,
        latest_processed_file_date: str
):
    return {
        object_name_pattern: latest_processed_file_date
        for object_name_pattern in [data_config.s3_object_key_pattern]
    }


def update_object_latest_dates(
        obj_pattern_with_latest_dates: dict,
        object_pattern: str,
        file_modified_timestamp,
):
    new_obj_pattern_with_latest_dates = {
        **{key: convert_datetime_to_string(value)
           for key, value
           in obj_pattern_with_latest_dates.items()},
        object_pattern: convert_datetime_to_string(file_modified_timestamp)
    }
    return new_obj_pattern_with_latest_dates
