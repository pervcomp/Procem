# -*- coding: utf-8 -*-
"""This module handles the periodic resending of failed udp sends to the ProCem RTL."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import adapters.common_utils as common_utils
import threading
import time
import sys

# the default configurations filename
CONF_FILE = "udp_send_checker.json"


def long_sleep(seconds):
    """Sleeps the given number of seconds."""
    max_interval = 3600.0  # sleep in one hour intervals
    start = time.time()
    while time.time() - start < seconds:
        interval = min(max_interval, seconds - (time.time() - start))
        time.sleep(interval)


def failed_sends_handler(resend_interval, adapter_folder=None):
    """Handles the failed UDP sends by periodically reading the backup file
       and sending that data to procem_rtl.
    """
    if not common_utils.USE_FILE_BACKUP:
        return
    if adapter_folder is not None:
        common_utils.BackupFileHandler.changeDirectory(adapter_folder)

    while True:
        resend_data = common_utils.BackupFileHandler.readData()

        if resend_data > 0:
            print(common_utils.getTimeString(), "Resend", resend_data, "measurements.")
        long_sleep(resend_interval)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        PROCEM_CONF_FILE = sys.argv[1]
    elif len(sys.argv) != 1:
        print("Start this program with 'python3", sys.argv[0], "config_file.json' command")
        print("or use 'python3 ", sys.argv[0], "' to use the default configuration filename: ", CONF_FILE, sep="")
        quit()

    # read configuration file
    # TODO: add sanity checks for the parameters
    jd = common_utils.readConfig(CONF_FILE)

    # the time interval in seconds between failed UDP send checks
    resend_interval = jd.get("resend_interval_s", 1200)
    adapter_folder = jd.get("adapter_folder", None)

    # start the failed UDP sends handler
    udp_thread = threading.Thread(target=failed_sends_handler, daemon=True, args=(resend_interval, adapter_folder))
    udp_thread.start()
    udp_thread.join()
