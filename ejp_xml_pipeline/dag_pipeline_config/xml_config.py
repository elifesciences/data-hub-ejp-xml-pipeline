import os
from pathlib import Path
from ejp_xml_pipeline.model.entities import (
    ManuscriptVersion,
    PersonV2,
    Person,
    Manuscript
)


class EntityDBLoadConfig:
    def __init__(
            self,
            file_name: str,
            table_name,
            s3_object_prefix: str
    ):
        self.file_name = file_name
        self.table_name = table_name
        self.file_directory = None
        obj_name = (
            s3_object_prefix.strip()
            if s3_object_prefix.strip().endswith('/')
            else s3_object_prefix.strip() + '/'
        )
        self.s3_object_prefix = obj_name + file_name + '/'
        self.s3_object_wildcard_prefix = self.s3_object_prefix + '*'

    def set_directory(self, file_directory):
        self.file_directory = file_directory

    def get_full_file_location(self):
        return os.fspath(
            Path(self.file_directory, self.file_name)
        )

    def set_directory_get_full_file_location(self, file_directory):
        self.set_directory(file_directory)
        return self.get_full_file_location()


# pylint: disable=invalid-name,too-many-instance-attributes
class eJPXmlDataConfig:
    def __init__(
            self,
            ejp_xml_config_dict: dict,
            deployment_env: str,
            environment_placeholder: str = "{ENV}"
    ):
        updated_config = (
            update_deployment_env_placeholder(
                original_dict=ejp_xml_config_dict,
                deployment_env=deployment_env,
                environment_placeholder=environment_placeholder
            )
        )
        self.dataset = updated_config.get("dataset")
        self.gcp_project = updated_config.get("gcpProjectName")
        self.s3_bucket = updated_config.get(
            "eJPXmlBucket", ""
        )
        self.s3_object_key_pattern = updated_config.get(
            "eJPXmlObjectKeyPattern", ""
        )
        self.etl_id = updated_config.get(
            "dataPipelineId",
        )
        self.state_file_bucket = updated_config.get(
            "stateFile", {}).get("bucket")
        self.state_file_object = updated_config.get(
            "stateFile", {}).get("object")
        self.manuscript_table = updated_config.get(
            "manuscriptTable"
        )
        self.manuscript_version_table = updated_config.get(
            "manuscriptVersionTable"
        )
        self.person_table = updated_config.get("personTable")
        self.person_v2_table = updated_config.get(
            "personVersion2Table"
        )
        self.temp_file_s3_bucket = updated_config.get(
            'tempS3FileStorage', {}
        ).get('bucket')
        self.temp_file_s3_obj_prefix = updated_config.get(
            'tempS3FileStorage', {}
        ).get('objectPrefix')
        self.entity_type_mapping = {
            ManuscriptVersion: EntityDBLoadConfig(
                ManuscriptVersion.__name__,
                self.manuscript_version_table,
                self.temp_file_s3_obj_prefix
            ),
            Manuscript: EntityDBLoadConfig(
                Manuscript.__name__,
                self.manuscript_table,
                self.temp_file_s3_obj_prefix
            ),
            Person: EntityDBLoadConfig(
                Person.__name__,
                self.person_table,
                self.temp_file_s3_obj_prefix
            ),
            PersonV2: EntityDBLoadConfig(
                PersonV2.__name__,
                self.person_v2_table,
                self.temp_file_s3_obj_prefix
            )
        }


def update_deployment_env_placeholder(
        original_dict: dict,
        deployment_env: str,
        environment_placeholder: str,
):
    new_dict = dict()
    for key, val in original_dict.items():
        if isinstance(val, dict):
            tmp = update_deployment_env_placeholder(
                val,
                deployment_env,
                environment_placeholder
            )
            new_dict[key] = tmp
        elif isinstance(val, list):
            new_dict[key] = [
                update_deployment_env_placeholder(
                    x,
                    deployment_env,
                    environment_placeholder
                )
                for x in val
            ]
        else:
            new_dict[key] = replace_env_placeholder(
                original_dict[key],
                deployment_env,
                environment_placeholder
            )
    return new_dict


def replace_env_placeholder(
        param_value,
        deployment_env: str,
        environment_placeholder: str
):
    new_value = param_value
    if isinstance(param_value, str):
        new_value = param_value.replace(
            environment_placeholder,
            deployment_env
        )
    return new_value
