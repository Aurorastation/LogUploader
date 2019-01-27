import logging
import zipfile
import requests
import datetime
import re
import os

logger = logging.getLogger('logUploader.logfile.processNew')

def process(full_name, file_data, params):
    base_name = file_data["date"] + "_" + file_data["gameid"]

    # Make sure we are not archiving the active file
    f = open(full_name, 'r')
    lines = f.readlines()
    f.close()
    last_line = lines[-1]
    ld = re.search(r"\[(?P<datetime>[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).[0-9]{3}.*", last_line)

    if not ld:
        logger.warning("Invalid format of last line in file: {}".format(full_name))
        return

    time_now = datetime.datetime.utcnow()
    time_log = datetime.datetime.strptime(ld.group("datetime"), "%Y-%m-%d %H:%M:%S")

    if time_now < time_log + datetime.timedelta(minutes=30):
        logger.info("Skipping File due to insufficient age: {}".format(full_name))
        return

    zipfile_path = os.path.join(params["archive_path"], base_name + ".zip")
    logger.debug("Creating ZIP File at: {}".format(zipfile_path))

    # Create a zip file
    zipf = zipfile.ZipFile(zipfile_path, "w", zipfile.ZIP_DEFLATED)
    zipf.write(full_name, base_name + ".log")
    zipf.close()

    # Upload zipped file
    upload_params = file_data
    upload_params["key"] = params["api_key"]
    files = {"logfile": open(zipfile_path, 'rb')}
    headers = {'Accept': 'application/json'}

    r = requests.post(params["api_url"], headers=headers, data=upload_params, files=files)

    # Delete the raw file if the upload was successful
    if r.status_code != 200:
        logger.warning("Error while uploading file: {}".format(r.text))
    else:
        os.remove(full_name)

    return 0