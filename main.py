import logging
import logging.config
import json
import os
import sys
import re

import logfile.processNew

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

    logger.info("Config file Loaded")

    count_files = 0
    count_processed = 0

    # Loop through the files in the log folder
    for root, d_names, f_names in os.walk(params["log_path"]):
        # Skip the runtime logs
        if "_runtime" in root:
            logger.debug("Skipping _runtime path: {}".format(root))
            continue
        for f_name in f_names:
            logger.debug("Checking if {} is properly named".format(f_name))
            count_files = count_files + 1
            full_name = os.path.join(root, f_name)
            stub_name = full_name.replace(params["log_path"], "")
            logger.debug("found full_name: {}".format(full_name))
            logger.debug("found stub_name: {}".format(stub_name))

            # Check if we have a properly named logfile:
            p = re.compile(
                r"^\/(?P<year>[0-9]{4})\/(?P<month>[0-9]{2})\/(?P<day>[0-9]{2})_(?P<gameid>[a-zA-Z0-9]{3}-[a-zA-Z0-9]{4})\.log$")
            m = p.search(stub_name)
            if m:
                file_date = "{}-{}-{}".format(m.group("year"), m.group("month"), m.group("day"))
                count_processed = count_processed + 1
                file_data = {
                    "date": file_date,
                    "gameid": m.group("gameid")
                }
                logger.debug("Processing file {} with data: {}".format(stub_name, file_data))

                # If so, then process it
                logfile.processNew.process(full_name, file_data,params)

    logger.info("Processed {}/{} files".format(count_processed, count_files))
    return 0


# this is the standard boilerplate that calls the main() function
if __name__ == '__main__':
    sys.exit(main(sys.argv))  # used to give a better look to exists
