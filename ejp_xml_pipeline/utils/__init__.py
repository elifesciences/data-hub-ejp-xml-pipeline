import yaml


class NamedDataPipelineLiterals:
    DAG_RUN = 'dag_run'
    RUN_ID = 'run_id'
    DAG_RUNNING_STATUS = 'running'
    S3_FILE_METADATA_NAME_KEY = "Key"
    S3_FILE_METADATA_LAST_MODIFIED_KEY = "LastModified"
    DEFAULT_AWS_CONN_ID = "aws_default"
    EJP_XML_CONFIG_FILE_PATH_ENV_NAME = (
        "EJP_XML_CONFIG_FILE_PATH"
    )
    EJP_XML_SCHEDULE_INTERVAL_ENV_NAME = (
        "EJP_XML_SCHEDULE_INTERVAL"
    )


def get_yaml_file_as_dict(file_location: str) -> dict:
    with open(file_location, 'r') as yaml_file:
        return yaml.safe_load(yaml_file)
