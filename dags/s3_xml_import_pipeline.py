import json
import logging
import os
from datetime import timedelta, datetime, timezone
from typing import Iterable, Tuple
from airflow import DAG
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.operators.python import ShortCircuitOperator

from ejp_xml_pipeline.dag_pipeline_config.xml_config import eJPXmlDataConfig
from ejp_xml_pipeline.etl_state import get_stored_ejp_xml_processing_state
from ejp_xml_pipeline.etl import (
    etl_ejp_xml_zip,
    download_load2bq_cleanup_temp_files
)
from ejp_xml_pipeline.utils import (
    NamedDataPipelineLiterals as named_literals,
    get_yaml_file_as_dict
)
from ejp_xml_pipeline.etl_state import (
    update_state,
    update_object_latest_dates,
)
from ejp_xml_pipeline.utils.dags.airflow_s3_util_extension import (
    S3NewKeyFromLastDataDownloadDateSensor,
    S3HookNewFileMonitor
)
from ejp_xml_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
    create_python_task
)

LOGGER = logging.getLogger(__name__)


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

NOTIFICATION_EMAILS_ENV_NAME = "AIRFLOW_NOTIFICATION_EMAIL_XML_LIST"

DAG_ID = "S3_XML_Data_Pipeline"


def get_default_args_with_notification_emails():
    notification_emails = os.getenv(
        NOTIFICATION_EMAILS_ENV_NAME, ""
    )
    default_args = get_default_args()
    if notification_emails:
        notification_emails = [
            mail.strip() for mail
            in notification_emails.split(",")
        ]
        default_args = {
            **default_args,
            'email': notification_emails
        }
    return default_args


S3_XML_ETL_DAG = DAG(
    dag_id=DAG_ID,
    schedule_interval=os.getenv(
        named_literals.EJP_XML_SCHEDULE_INTERVAL_ENV_NAME
    ),
    default_args=get_default_args_with_notification_emails(),
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=20,
    max_active_tasks=30
)


def get_variable_key(pipeline_id):
    return DAG_ID + pipeline_id


def get_config() -> eJPXmlDataConfig:
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV_VALUE
    )
    conf_file_path = os.environ[named_literals.EJP_XML_CONFIG_FILE_PATH_ENV_NAME]

    data_config_dict = get_yaml_file_as_dict(
        conf_file_path
    )
    return eJPXmlDataConfig(data_config_dict, dep_env)


# pylint: disable='unused-argument'
def is_dag_etl_running(**context):
    data_config = get_config()

    dag_run_var_value = Variable.get(
        get_variable_key(data_config.etl_id), None
    )

    if dag_run_var_value:
        dag_run_var_value_dict = json.loads(dag_run_var_value)
        prev_run_id = dag_run_var_value_dict.get(
            named_literals.RUN_ID
        )
        dag_runs = DagRun.find(dag_id=DAG_ID, run_id=prev_run_id)
        if (
                len(dag_runs) > 0 and
                (dag_runs[0]).get_state()
                == named_literals.DAG_RUNNING_STATUS
        ):
            return False

    return True


def update_prev_run_id_var_val(**context):
    data_config = get_config()
    run_id = context.get(named_literals.RUN_ID)
    Variable.set(
        get_variable_key(data_config.etl_id),
        json.dumps({named_literals.RUN_ID: run_id})
    )


def etl_new_ejp_xml_files(**context):
    data_config = get_config()
    obj_pattern_with_latest_dates = (
        get_stored_ejp_xml_processing_state(
            data_config,
            get_default_initial_s3_last_modified_date()
        )
    )
    matching_file_metadata_iter = etl_s3_object_pattern(
        data_config,
        obj_pattern_with_latest_dates,
        data_config.s3_bucket,
    )

    for matching_file_metadata, object_key_pattern in matching_file_metadata_iter:
        object_key = matching_file_metadata.get(
            named_literals.S3_FILE_METADATA_NAME_KEY
        )
        etl_ejp_xml_zip(
            data_config, object_key,
        )

        updated_obj_pattern_with_latest_dates = (
            update_object_latest_dates(
                obj_pattern_with_latest_dates,
                object_key_pattern,
                matching_file_metadata.get(
                    named_literals.S3_FILE_METADATA_LAST_MODIFIED_KEY
                )
            )
        )
        update_state(
            updated_obj_pattern_with_latest_dates,
            data_config.state_file_bucket,
            data_config.state_file_object
        )


def load_temp_ejp_json_files_to_bq(**context):
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


def etl_s3_object_pattern(
        data_config: eJPXmlDataConfig,
        obj_pattern_with_latest_dates: dict,
        s3_bucket_name: str
) -> Iterable[Tuple]:

    hook = S3HookNewFileMonitor(
        aws_conn_id=named_literals.DEFAULT_AWS_CONN_ID,
        verify=None
    )
    new_s3_files = hook.get_new_object_key_names(
        obj_pattern_with_latest_dates,
        s3_bucket_name
    )

    for object_key_pattern, matching_files_list in new_s3_files.items():
        sorted_matching_files_list = (
            sorted(
                matching_files_list,
                key=lambda file_meta:
                file_meta[
                    named_literals.S3_FILE_METADATA_LAST_MODIFIED_KEY
                ]
            )
        )

        for object_index, matching_file_metadata in enumerate(sorted_matching_files_list):
            object_key = matching_file_metadata.get(
                named_literals.S3_FILE_METADATA_NAME_KEY
            )
            s3_bucket = (
                data_config.temp_file_s3_bucket
                if object_key.endswith('.json')
                else data_config.s3_bucket
            )
            LOGGER.info(
                'processing file (%d / %d): s3://%s/%s',
                1 + object_index,
                len(sorted_matching_files_list),
                s3_bucket,
                object_key
            )
            yield matching_file_metadata, object_key_pattern


def get_default_initial_s3_last_modified_date():
    return os.getenv(
        INITIAL_S3_XML_FILE_LAST_MODIFIED_DATE_ENV_NAME,
        DEFAULT_INITIAL_S3_XML_FILE_LAST_MODIFIED_DATE
    )


SHOULD_REMAINING_TASK_EXECUTE = ShortCircuitOperator(
    task_id='Should_Remaining_Tasks_Execute',
    python_callable=is_dag_etl_running,
    dag=S3_XML_ETL_DAG)


NEW_S3_FILE_SENSOR = S3NewKeyFromLastDataDownloadDateSensor(
    task_id='S3_Key_Sensor_Task',
    poke_interval=60 * int(
        os.getenv(
            S3_BUCKET_POLLING_INTERVAL_IN_MINUTES_ENV_VAR_NAME, "5"
        )
    ),
    timeout=60 * int(
        os.getenv(
            S3_BUCKET_POLLING_TIMEOUT_IN_MINUTES_ENV_VAR_NAME,
            "120"
        )
    ),
    retries=0,
    state_info_extract_from_config_callable=get_stored_ejp_xml_processing_state,
    default_initial_s3_last_modified_date=(
        get_default_initial_s3_last_modified_date()
    ),
    dag=S3_XML_ETL_DAG,
    deployment_environment=os.getenv(
        DEPLOYMENT_ENV_ENV_NAME,
        DEFAULT_DEPLOYMENT_ENV_VALUE
    )
)


LOCK_DAGRUN_UPDATE_PREVIOUS_RUNID = create_python_task(
    S3_XML_ETL_DAG, "Update_Previous_RunID_Variable_Value_For_DagRun_Locking",
    update_prev_run_id_var_val,
)

ETL_XML_TO_S3_JSON = create_python_task(
    S3_XML_ETL_DAG, "ETL_eJP_XML_To_S3_JSON",
    etl_new_ejp_xml_files,
    email_on_failure=True
)


LOAD_TEMP_FILE_TO_BQ = create_python_task(
    S3_XML_ETL_DAG, "Load_S3_JSON_To_BQ",
    load_temp_ejp_json_files_to_bq,
    email_on_failure=True
)

# pylint: disable=pointless-statement
(
        SHOULD_REMAINING_TASK_EXECUTE >>
        LOCK_DAGRUN_UPDATE_PREVIOUS_RUNID >>
        NEW_S3_FILE_SENSOR >>
        ETL_XML_TO_S3_JSON >>
        LOAD_TEMP_FILE_TO_BQ
)
