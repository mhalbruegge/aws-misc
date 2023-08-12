from botocore.exceptions import NoCredentialsError
from concurrent.futures import ThreadPoolExecutor

import boto3
import os


def upload_to_s3(bucket_name, local_path, s3_path):
    s3 = boto3.client("s3")
    try:
        s3.upload_file(local_path, bucket_name, s3_path)
        print(f"Upload from {local_path} to {s3_path} succeeded.")
    except NoCredentialsError:
        print("No AWS credentials were found.")
        raise


def download_from_s3(bucket_name, s3_path, local_path):
    s3 = boto3.client("s3")
    try:
        s3.download_file(bucket_name, s3_path, local_path)
        print(f"Download from {s3_path} to {local_path} succeeded.")
    except NoCredentialsError:
        print("No AWS credentials were found.")
        raise


def download_all_files_in_s3_directory(bucket_name, s3_directory, local_directory):
    s3 = boto3.client("s3")
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_directory)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for s3_object in objects["Contents"]:
            s3_path = s3_object["Key"]
            if s3_path == s3_directory:
                continue  # Skip this iteration
            local_path = os.path.join(
                local_directory, s3_path.replace(s3_directory, "")
            )
            futures.append(executor.submit(download_from_s3, s3_path, local_path))

        for future in futures:
            future.result()
