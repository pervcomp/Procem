# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import socket
import threading
import socketserver
import time
import random

PROCEM_SERVER_IP = "127.0.0.1"
PROCEM_SERVER_PORT = 6666

ADAPTER_IP = "127.0.0.1"
# NOTE: Port should be unique for all adapters, would be better to let OS assign it
ADAPTER_PORT = 6667

# Python string Formatter class formatted string for IoTTicket compatible message
# to get packet string the packet, call IOTTicketPKT.format(value,timestamp)
# The role of registering the devices is left to the Procem server that relays these to IOTTicket
IOTTicketPKT = '{{"name": "RandomNumber", "path": "Procem", "v": {0:d}, "ts": {1:d}, "unit": "Num", ' + \
               '"dataType": "long", "variableNumber": 3, "confidential": false }}'


# Thanks stackoverflow for this nice little lambda function
# WARNING: python is not accurate on sleeping, but depends on OS and interperter implementation
def usleep(x):
    time.sleep(x/1000000.0)


def sendUDPmsg(ip, port, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.sendto(bytes(message, "utf-8"), (ip, port))
    finally:
        sock.close()


# A callback class to handle incoming UDP packets (that procem server sends)
class ReceivedUDPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request[0].strip()
        socket = self.request[1]
        print("{} wrote:".format(self.client_address[0]))
        print(data)


def startUDPserver():
    server = socketserver.ThreadingUDPServer((ADAPTER_IP, ADAPTER_PORT), ReceivedUDPHandler)
    ip, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    return server


def stopUDPserver(server):
    server.shutdown()

if __name__ == "__main__":
    # start reply channel
    # srv = startUDPserver()
    while(0 < 1):
        # TODO: read meter, send message
        # IoTTicket uses ms granularity
        print(".")
        pkt = IOTTicketPKT.format(random.randint(0, 100), int(time.mktime(time.localtime()))*1000)
        sendUDPmsg(PROCEM_SERVER_IP, PROCEM_SERVER_PORT, pkt)
        usleep(1000000)
    # stopUDPserver(srv)
