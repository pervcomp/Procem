# -*- coding: utf-8 -*-
"""This module contains a program for sending data to the battery simulation."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import copy
import datetime
import json
import queue
import struct
import sys
import threading
import time

import adapters.common_utils as common_utils
import utils.datastorage as datastorage
import tcp_utils

CONFIG_FILE = "battery_data.json"
ENDIAN = "<"  # use little endian when packing values


def dataHandler(data_queue, data_storage, rtl_ids, time_limit, target_address):
    """Data handler that puts all the items from the queue to the data storage."""
    pack_code = ENDIAN + str(len(rtl_ids)) + "f"
    ip, port = target_address

    count = 0
    last_send_time = time.time()
    while True:
        item = data_queue.get()
        if item is None:
            break

        try:
            jd = json.loads(item)
            data_storage.add_value(jd["id"], jd["v"], jd["ts"])

            # send udp message containing the snapshot to the simulation machine
            # if enough time has passed since the last send
            if last_send_time + time_limit < time.time():
                stored_ids = sorted(data_storage.get_ids())
                if sorted(stored_ids) != sorted(rtl_ids):
                    print("Only ids", stored_ids, "received so far")
                else:
                    snapshot = [float(data_storage.get_value(rtl_id).v) for rtl_id in rtl_ids]
                    packed_snapshot = struct.pack(pack_code, *snapshot)

                    common_utils.sendUDPmsg(ip, port, message=packed_snapshot, use_confirmation=False)
                    count += 1
                    last_send_time = time.time()

                    if count % 10000 == 0:
                        print(common_utils.getTimeString(), "UDP messages sent:", count)

        except Exception as error:
            print(error)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        CONFIG_FILE = sys.argv[1]
    elif len(sys.argv) != 1:
        print("Start this program with 'python3", sys.argv[0], "config_file.json' command")
        print("or use 'python3 ", sys.argv[0], "' to use the default configuration filename: ", CONFIG_FILE, sep="")
        quit()

    config = common_utils.readConfig(CONFIG_FILE)
    time_limit = config["time_limit"]
    rtl_ids = config["rtl_ids"]
    target_ip = config["target_ip"]
    target_port = config["target_port"]

    data_queue = queue.Queue()
    data_storage = datastorage.DataStorage(1)  # store only the most recent value for each measurement

    address = (tcp_utils.BATTERY_SERVER_IP, tcp_utils.BATTERY_SERVER_PORT)
    server = tcp_utils.ThreadedTCPServer(address, tcp_utils.handlerFactory(data_queue))

    server_thread = threading.Thread(target=server.serve_forever, name="BatteryDemo_TCP_Server")
    server_thread.daemon = True
    server_thread.start()
    print("TCP Server loop running in thread:", server_thread.name)

    queue_thread = threading.Thread(
        target=dataHandler,
        name="BatteryDemo_DataHandler",
        kwargs={
            "data_queue": data_queue,
            "data_storage": data_storage,
            "rtl_ids": rtl_ids,
            "time_limit": time_limit,
            "target_address": (target_ip, target_port)
        }
    )
    queue_thread.start()

    while True:
        txt = input("Press enter key to end:\n\r")
        if not txt:
            server.shutdown()
            data_queue.put(None)
            break
