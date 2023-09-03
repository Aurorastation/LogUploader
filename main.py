import datetime
import json
import logging
import logging.config
import os
import shutil
import sys
import time
import zipfile

import boto3
import botocore.exceptions
import requests
from boto3.s3.transfer import S3Transfer
from boto3.s3.transfer import TransferConfig


def main(argv):
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('logUploader')

    logger.info("LogUploader Started")

    # Load the config
    params = {}
    try:
        with open('config.json', 'r') as f:
            params = json.load(f)
    except Exception as ex:
        logger.error("Error while loading config.json. " + ex)
        return 1

    if not validate_config(params):
        return 1

    logger.info("Config file Loaded")

    count_found = 0
    count_processed = 0

    # Loop through the files in the log folder
    logger.debug("Checking Path: {}".format(params["log_path"]))
    for folder in os.listdir(params["log_path"]):
        if not os.path.isdir(os.path.join(params["log_path"], folder)):
            continue
        count_found = count_found + 1
        # ToDo: Add a Process Folder function and one to (re-) process already existing zip files?
        fullpath = os.path.join(params["log_path"], folder)
        logger.debug("Found directory: {}".format(fullpath))

        # Check if files in folder are newer than 30minutes
        latest_time = get_newest_mtime_in(fullpath)
        dt_object = datetime.datetime.fromtimestamp(latest_time)
        time_formatted = dt_object.strftime("%Y-%m-%d")
        time_diff = (time.time() - latest_time) / 60
        logger.debug("Last mtime for folder {}: {}".format(folder, time_formatted))
        logger.debug("Time diff for folder {}: {} min".format(folder, time_diff))
        if time_diff < params["time_before_upload_min"]:
            logger.info(
                "Last Log files in {} are too recent: {:.2f} min. Need to be {} - Skipping further processing".format(
                    folder, time_diff, params["time_before_upload_min"]))
            continue

        # Zip Log
        zip_name = f'{time_formatted}-{folder}.zip'
        zip_fullpath = os.path.join(params["log_path"], zip_name)
        if os.path.exists(zip_fullpath):
            logger.warning("Zip File {} already exists - Skipping further processing".format(zip_fullpath))
            # ToDo: Check if we should skip only the ziping (and still upload/register it)
            continue
        zip_folder(fullpath, zip_fullpath)
        # Delete source folder
        shutil.rmtree(fullpath)
        logger.debug("Deleted Folder after ZIPing: {}".format(fullpath))

        # Upload Log to S3
        s3_path = f'{params["s3_bucket_path"]}/{zip_name}'
        if not upload_archive(zip_fullpath, params["s3_bucket"], s3_path,
                              params["s3_url"], params["s3_access_key"], params["s3_secret_key"],
                              params["s3_max_bandwidth_mb"]):
            continue

        # Post info to WI
        if not notify_wi(s3_path, folder, time_formatted, params["wi_url"], params["wi_key"]):
            continue

        # ToDo: Check if its uploaded to S3 and if it exists in the WI

        # Delete/Archive File
        if params["archive"] == 1:
            logger.debug("Archiving File after processing has been completed: {}".format(zip_fullpath))
            # Check if archive path exists
            shutil.move(zip_fullpath, os.path.join(params["archive_path"], zip_name))
        else:
            logger.debug("Deleting File after processing has been completed: {}".format(zip_fullpath))
            os.remove(zip_fullpath)

        count_processed = count_processed + 1
        logger.info("Folder {} has been processed successfully".format(folder))

    logger.info("Processed {}/{} files".format(count_processed, count_found))
    return 0


def validate_config(params):
    logger = logging.getLogger('logUploader')
    # Check if the required config values are present
    required_keys = ["s3_url", "s3_bucket", "s3_bucket_path", "s3_secret_key", "s3_access_key", "s3_max_bandwidth_mb",
                     "log_path", "archive_path", "archive", "wi_url", "wi_key", "time_before_upload_min"]
    for key in required_keys:
        if key not in params:
            logger.error("Configuration Key: {} is missing".format(key))
            return False

    # if archiving is enabled make sure that the archive path exists
    if params["archive"] == 1:
        if not os.path.exists(params["archive_path"]):
            logger.error("Archiving ie enabled, but archive path does not exist: {}".format(params["archive_path"]))
            return False

    logger.debug("Config Validation passed")
    return True


def get_newest_mtime_in(path: str):
    latest_time = 0
    for root, d_names, f_names in os.walk(path):
        for f_name in f_names:
            time = os.path.getmtime(os.path.join(root, f_name))
            if time > latest_time:
                latest_time = time
    return latest_time


def zip_folder(folder_path, output_path):
    logger = logging.getLogger('logUploader')
    logger.debug('Zipping %s to %s', folder_path, output_path)

    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)

    logger.debug('%s successfully zipped to %s', folder_path, output_path)


def upload_archive(local_path: str, bucket_name: str, s3_path: str, s3_endpoint, s3_access_key, s3_secret_key,
                   max_bandwidth_mb):
    logger = logging.getLogger('logUploader')
    logger.debug('Uploading file {} to S3 Bucket {} with path {}'.format(local_path, bucket_name, s3_path))

    # Creates a session using the S3 credentials
    session = boto3.Session(
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key
    )
    transfer_config = TransferConfig(
        max_bandwidth=max_bandwidth_mb * 1024
    )

    # Check if the file exists
    s3 = boto3.resource('s3',
                        endpoint_url=s3_endpoint,
                        aws_access_key_id=s3_access_key,
                        aws_secret_access_key=s3_secret_key)

    exists = False
    try:
        s3.Object(bucket_name, s3_path).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            exists = False
        else:
            # Something else has gone wrong.
            raise
    else:
        exists = True
        logger.error("File {} already exists in S3 - Skipping Upload / Further Processing".format(s3_path))
        return False

    # Opens the client, pointing to the endpoint, and upload the file at the specified path
    s3_client = session.client('s3', endpoint_url=s3_endpoint)
    # Transfer the actual file
    s3_transfer = S3Transfer(s3_client, config=transfer_config)
    s3_transfer.upload_file(local_path, bucket_name, s3_path)

    logger.debug('Log %s successfully uploaded to bucket %s at %s', local_path, bucket_name, s3_path)
    return True


def notify_wi(s3_path: str, game_id: str, date: str, wi_url: str, wi_key: str):
    logger = logging.getLogger('logUploader')
    r = requests.post(wi_url, data={
        'filepath': s3_path,
        'date': date,
        'gameid': game_id,
        'key': wi_key
    })
    # ToDo: Expand Error Checking
    if r.status_code == 200:
        logger.debug("Successfully Updated WI. s3_path: {} game_id: {} date: {}".format(s3_path, game_id, date))
        return True
    else:
        logger.error("Error While Updating WI. Error-Code: {} Error-Message: {}".format(r.status_code, r.text))
        return False


if __name__ == '__main__':
    sys.exit(main(sys.argv))  # used to give a better look to exists
