import yaml


class NamedDataPipelineLiterals:
    DAG_RUNNING_STATUS = 'running'
    S3_FILE_METADATA_NAME_KEY = "Key"
    EJP_XML_CONFIG_FILE_PATH_ENV_NAME = (
        "EJP_XML_CONFIG_FILE_PATH"
    )


def get_yaml_file_as_dict(file_location: str) -> dict:
    with open(file_location, 'r', encoding="UTF-8") as yaml_file:
        return yaml.safe_load(yaml_file)
