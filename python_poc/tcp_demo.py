# -*- coding: utf-8 -*-
"""This module contains a demo program for demonstrating how data can be received from Procem rtl program
   using a TCP connection. It might be used as a basis for the Procem battery demo main class."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import copy
import json
import queue
import threading
import time

import utils.datastorage as datastorage
import tcp_utils

TIME_INTERVAL = 900.0
VALUE_LIMIT = 86400


def dataHandler(data_queue, data_storage):
    """Data handler that puts all the items from the queue to the data storage."""
    while True:
        item = data_queue.get()
        if item is None:
            break

        try:
            jd = json.loads(item)
            data_storage.add_value(jd["id"], jd["v"], jd["ts"])
        except:
            pass


def printHandler(data_storage):
    """Print handler that print outs some statistics from the data storage for demonstration purposes."""
    while True:
        time.sleep(TIME_INTERVAL)
        ids = data_storage.get_ids()
        for rtl_id in ids:
            values = data_storage.get_values(rtl_id)
            if values is None or len(values) == 0:
                continue
            latest_value = values[len(values) - 1]
            latest_v = "{:>6.3f}".format(round(latest_value.v, 3))
            latest_ts = time.localtime(latest_value.ts / 1000)
            avg_value = round(sum([x.v for x in values]) / len(values), 3)
            max_value = round(max([x.v for x in values]), 3)
            print("ID: {:>5d} total: {:>5d} avg: {:>6.3f} max: {:>6.3f} latest: ({}, {:0>2d}:{:0>2d}:{:0>2d})".format(
                rtl_id, len(values), avg_value, max_value, latest_v,
                latest_ts.tm_hour, latest_ts.tm_min, latest_ts.tm_sec))
        print()


if __name__ == "__main__":
    data_queue = queue.Queue()
    data_storage = datastorage.DataStorage(VALUE_LIMIT)

    address = (tcp_utils.BATTERY_SERVER_IP, tcp_utils.BATTERY_SERVER_PORT)
    server = tcp_utils.ThreadedTCPServer(address, tcp_utils.handlerFactory(data_queue))

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    print("TCP Server loop running in thread:", server_thread.name)

    queue_thread = threading.Thread(
        target=dataHandler,
        kwargs={"data_queue": data_queue, "data_storage": data_storage})
    queue_thread.start()

    print_thread = threading.Thread(
        target=printHandler,
        kwargs={"data_storage": data_storage})
    print_thread.daemon = True
    print_thread.start()

    while True:
        txt = input("Press enter key to end:\n\r")
        if not txt:
            server.shutdown()
            data_queue.put(None)
            break
