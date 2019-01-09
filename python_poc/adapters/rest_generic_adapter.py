# -*- coding: utf-8 -*-
"""Module for starting an adapters that read data from REST APIs."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import importlib
import queue
import sys
import threading
import time

try:
    import adapters.common_utils as common_utils
except:
    # used when running the module directly
    import common_utils

DEFAULT_CONFIG_SCHEME = "rest_api_configuration.json"


def generic_website_worker(worker, website, website_queue):
    worker_object = worker(website, website_queue)
    name = website.get("name", "Unknown")
    verbose_limit = website.get("verbose", 0)

    success_count = 0
    while True:
        wait_time = worker_object.getWaitingTime()
        # print(common_utils.getTimeString(), name, "worker going to sleep for", round(wait_time, 1), "seconds.")
        time.sleep(wait_time)

        success = worker_object.getData()
        if success:
            success_count += 1
            if 0 < verbose_limit <= success_count:
                print(common_utils.getTimeString(), " Data from ", name, " worker", sep="", end="")
                if success_count > 1:
                    print(",", success_count, "times.")
                else:
                    print(".")
                success_count = 0
        else:
            print(common_utils.getTimeString(), "No data from", name, "worker.")


if __name__ == "__main__":
    if len(sys.argv) == 3:
        website_config_filename = sys.argv[1]
        config_scheme_filename = sys.argv[2]
    elif len(sys.argv) == 2:
        website_config_filename = sys.argv[1]
        config_scheme_filename = DEFAULT_CONFIG_SCHEME
    else:
        print("Start this adapter with 'python3", sys.argv[0], "website_config.json (config_scheme.json) command")
        website_config_filename = ""
        config_scheme_filename = DEFAULT_CONFIG_SCHEME
        quit()

    # read configuration information from the configuration files
    print("Reading configurations")
    websites = common_utils.readConfig(website_config_filename)
    configurations = common_utils.readConfig(config_scheme_filename)

    # start the data queue used to send data to Procem
    data_queue = queue.Queue()
    threading.Thread(target=common_utils.procemSendWorker, kwargs={"data_queue": data_queue}).start()

    for website_id, current_website in websites.items():
        try:
            website_conf_name = current_website["configuration"]
            website_conf = configurations[website_conf_name]
            current_website["config"] = website_conf
            website_module_name = website_conf["worker"]["module"]
            website_module = importlib.import_module(website_module_name)
            website_worker_name = website_conf["worker"]["name"]
            website_worker = getattr(website_module, website_worker_name, None)
            current_website["name"] = website_id
        except Exception as error:
            print(error)
            website_worker = None

        if website_worker is not None:
            print("Starting thread for REST API: ", website_id, sep="")
            time.sleep(1.0)
            website_thread = threading.Thread(
                target=generic_website_worker,
                kwargs={"worker": website_worker, "website": current_website, "website_queue": data_queue},
                daemon=True)
            website_thread.start()

    while True:
        txt = input("Press enter key to end:\n\r")
        if not txt:
            data_queue.put(None)
            break
