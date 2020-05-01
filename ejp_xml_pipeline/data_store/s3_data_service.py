import json
from contextlib import contextmanager
import boto3


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


def delete_s3_object(bucket, object_key):
    s3_client = boto3.client('s3')
    s3_client.delete_object(
        Bucket=bucket,
        Key=object_key
    )
