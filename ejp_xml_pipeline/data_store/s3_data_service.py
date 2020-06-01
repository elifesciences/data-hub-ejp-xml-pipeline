import json
import logging
from contextlib import contextmanager
import boto3
from botocore.exceptions import ClientError


@contextmanager
def s3_open_binary_read(bucket: str, object_key: str):
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    streaming_body = response["Body"]
    try:
        yield streaming_body
    finally:
        streaming_body.close()


def download_s3_json_object(bucket: str, object_key: str) -> dict:
    with s3_open_binary_read(
            bucket=bucket, object_key=object_key
    ) as streaming_body:
        return json.load(streaming_body)


def upload_s3_object(bucket: str, object_key: str, data_object) -> bool:
    s3_client = boto3.client("s3")
    s3_client.put_object(Body=data_object, Bucket=bucket, Key=object_key)
    return True


def upload_file_into_s3(bucket: str, object_key: str, full_file_path: str) -> bool:
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(full_file_path, bucket, object_key)
    except ClientError as err:
        logging.error(err)
        return False
    return True


def download_s3_object_as_string(
        bucket: str, object_key: str
) -> str:
    with s3_open_binary_read(
            bucket=bucket, object_key=object_key
    ) as streaming_body:
        file_content = streaming_body.read()
        return file_content.decode("utf-8")


def delete_s3_objects(bucket, keys):
    s3_client = boto3.client('s3')
    if not isinstance(keys, list):
        keys = [keys]
    for key in keys:
        s3_client.delete_object(
            Bucket=bucket,
            Key=key
        )
