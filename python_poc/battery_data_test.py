# -*- coding: utf-8 -*-
"""This module contains a test for the battery data sender."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import datetime
import json
import socketserver
import struct
import threading

PACK_CODE = None


class ReceivedUDPHandler(socketserver.BaseRequestHandler):
    """A callback handler to handle incoming UDP packets."""
    def handle(self):
        packed_data = self.request[0]
        data = struct.unpack(PACK_CODE, packed_data)
        print(datetime.datetime.now(), data)


if __name__ == "__main__":
    config_file = "battery_data.json"
    with open(config_file, 'r') as file:
        config = json.load(file)

    rtl_ids = config["rtl_ids"]
    PACK_CODE = "<" + str(len(rtl_ids)) + "f"

    target_ip = config["target_ip"]
    target_port = config["target_port"]

    # for receiving data from the adapter programs
    server = socketserver.ThreadingUDPServer((target_ip, target_port), ReceivedUDPHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    while True:
        txt = input("Press enter key to end:\n\r")
        if txt == "":
            break

    server.shutdown()
