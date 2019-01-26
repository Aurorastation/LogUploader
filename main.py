import logging
import json
import os
import sys
import re
import zipfile
import requests

params = {}
logger = None

def process_file(full_name, file_data):
    global params
    global logger

    base_name = file_data["date"] + "_" + file_data["gameid"]

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


def main(argv):
    global logger
    global params

    # Setup logging
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    # Load the config
    try:
        with open('config.json', 'r') as f:
            params = json.load(f)
    except Exception as ex:
        print(ex)
        return 1

    # Loop through the files in the log folder
    for root, d_names, f_names in os.walk(params["log_path"]):
        # Skip the runtime logs
        if "_runtime" in root:
            continue
        for f_name in f_names:
            full_name = os.path.join(root, f_name)
            stub_name = full_name.replace(params["log_path"], "")
            logger.debug("full_name: {}".format(full_name))

            # Check if we have a properly named logfile:
            p = re.compile(
                r"^\\(?P<year>[0-9]{4})\\(?P<month>[0-9]{2})\\(?P<day>[0-9]{2})_(?P<gameid>[a-zA-Z0-9]{3}-[a-zA-Z0-9]{4})\.log$")
            m = p.search(stub_name)
            if m:
                file_data = {
                    "date": "{}-{}-{}".format(m.group("year"), m.group("month"), m.group("day")),
                    "gameid": m.group("gameid")
                }
                logger.debug("data for file: {}".format(file_data))

                # If so, then process it
                process_file(full_name, file_data)

    return 0


# this is the standard boilerplate that calls the main() function
if __name__ == '__main__':
    sys.exit(main(sys.argv))  # used to give a better look to exists
