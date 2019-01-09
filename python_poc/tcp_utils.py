# -*- coding: utf-8 -*-
"""This module contains classes for TCP client and TCP server for sending data using TCP connection such that
   if there is a connection problem that can be solved, the problem will not be visible to the user.
   To create TCP server use the function handlerFactory
       server = ThreadedTCPServer(address, handlerFactory(data_queue))
   where data_queue is the queue in which the server puts all received messages."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import queue
import socket
import socketserver
import threading

import adapters.common_utils as common_utils

# the TCP server address for the ProCem battery demo TCP server
BATTERY_SERVER_IP = "127.0.0.1"  # NOTE: change if not running in the same machine
BATTERY_SERVER_PORT = 7777  # NOTE: change to appropriate port number

CONNECTION_TRIES = 10  # the number of tries used when creating a connection
SEND_TRIES = 10  # the number of tries used when sending data packets
SOCKET_TIMEOUT = 0.5  # the idle time in seconds during data transfer that will result in connection failure

OK_MESSAGE = bytes("OK", "utf-8")  # the response message that is send after successful data transfer
END_MESSAGE = "END"  # the message that indicates for the server that it should be closed


class ThreadedTCPRequestHandler(socketserver.StreamRequestHandler):
    """TCP handler that puts the received data into the given data queue."""
    def __init__(self, data_queue, *args, **kwargs):
        self.data_queue = data_queue
        socketserver.StreamRequestHandler.__init__(self, *args, **kwargs)

    def handle(self):
        while True:
            try:
                data = self.rfile.readline().strip().decode("utf-8")
                if data == "":
                    continue
                elif data == END_MESSAGE:
                    print(common_utils.getTimeString(), "TCP server: closing connection")
                    return

                self.data_queue.put(data)
                sock = self.request
                sock.send(OK_MESSAGE)

            except:
                # TODO: add error handling
                pass


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Class for threaded TCP Server."""
    daemon_threads = True


class TCPClient:
    """A TCP Client class for sending messages without worrying about connection problems."""
    def __init__(self, ip, port):
        self.__address = (ip, port)
        self.__sock = None
        self.setupSocket()
        self.connect()

    def setupSocket(self):
        """Sets up the TCP socket for the client."""
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.settimeout(SOCKET_TIMEOUT)

    def connect(self, try_number=1):
        """Tries to connect the client to the TCP server in the given address."""
        if try_number > CONNECTION_TRIES:
            return
        try:
            self.__sock.connect(self.__address)
        except:
            self.setupSocket()
            self.connect(try_number + 1)

    def close(self):
        """Closes the connection to the TCP server."""
        try:
            self.send(END_MESSAGE)
            self.__sock.close()
        except:
            pass

    def send(self, message):
        """Sends a message to the TCP server. Return True if successful, and False otherwise."""
        byte_msg = bytes(str(message) + "\n", "utf-8")
        total_bytes_sent = 0
        try:
            while total_bytes_sent < len(byte_msg):
                sent = self.__sock.send(byte_msg[total_bytes_sent:])
                if sent == 0:
                    return False
                total_bytes_sent += sent

            resp = self.__sock.recv(len(OK_MESSAGE))
            return resp == OK_MESSAGE
        except:
            return False

    def robustSend(self, message):
        """Sends a message to the TCP server. If a problem occurs, tries again for a maximum of SEND_TRIES attempts.
           Return True if successful, and False otherwise."""
        success = False
        try_number = 1
        while not success and try_number <= SEND_TRIES:
            success = self.send(message)
            if not success:
                self.close()
                self.connect()
            try_number += 1

        return success


def handlerFactory(data_queue):
    """A helper function for creating request handlers with the given data queue."""
    def createHandler(*args, **kwargs):
        return ThreadedTCPRequestHandler(data_queue, *args, **kwargs)
    return createHandler
