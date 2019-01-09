# -*- coding: utf-8 -*-
"""This module contains the adapter for Electrix measurement device. It creates a TCP server and when
   a device connects to the server, reads data from it and sends it to be handled by ProCem RTL worker."""

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
import sys
import threading
import time

try:
    import adapters.common_utils as common_utils
    import adapters.mxelectrix_model as mxelectrix_model
    import adapters.mxelectrix_parser as mxelectrix_parser
except:
    # used when running the module directly
    import common_utils
    import mxelectrix_model
    import mxelectrix_parser

PROCEM_SERVER_IP = common_utils.PROCEM_SERVER_IP
PROCEM_SERVER_PORT = common_utils.PROCEM_SERVER_PORT

# The port and IP that the XPORT from MXElectrix device is configure to connect to
# NOTE: the MXELECTRIX_RECEIVER_IP can be:
# 1) IP of one of the ethernet adapters in the computer running this script
# OR
# 2) empty string "" so any available ethernet port will accept the connection
#
# these are read from a configurations file in main()
MXELECTRIX_RECEIVER_PORT = None
MXELECTRIX_RECEIVER_IP = None

# maximum size for UDP payload. Current value based on a quick experiment where it was 8192
UDP_MAX_SIZE = common_utils.UDP_MAX_SIZE

# To reduce UDP traffic, buffer the data sending to procem_rtl using this global queue
data_queue = queue.Queue()

dataModel = None

# The names of the configuration files from where the data model information is read.
# NOTE: these default filenames can be overwritten using configuration file.
CONFIG_SCHEME_FILE_NAME = "mxelectrix_model_config.json"
MEASUREMENT_ID_FILE_NAME = "Laatuvahti_measurement_IDs.csv"


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """A tcp server that handles each connection in its own thread and thus can handle measurements
       from multiple devices."""
    daemon_threads = True


# NOTE: currently the stream handling goes like this
# 1. wait for connect2 message from the device
#    - if not received in 10 tries, send esc and close commands and return (wait for new connection)
# 2. open the connection with open command
# 3. start the stream with "get_stream, rations" command
# 4. set the module id
# 5. set the stream format
# 6. read measurements until broken pipe
# This assumes that the measurement device is set to set_unlocked=Y, meaning no encryption
# TODO: AES encryption of Xport should be taken into use to add security
class MXElectrixTCPHandler(socketserver.StreamRequestHandler):
    def readLine(self):
        """Reads a line from the stream:"""
        return self.rfile.readline().strip().decode("utf-8")

    def writeCommand(self, command):
        """Writes a command to the stream."""
        self.wfile.write(bytes(command + "\r\n", "utf-8"))

    def handle(self):
        try:
            # this will send an exception if no response in 45 seconds
            self.request.settimeout(45)

            self.MXElectrixHandle()
        except socket.timeout:
            print(common_utils.getTimeString(), "ERROR: The socket timed out for", self.client_address[0])
            self.writeCommand("esc")
            self.writeCommand("close")

        except Exception as error:
            print(common_utils.getTimeString(), "ERROR:", self.client_address[0], error)
            self.writeCommand("esc")
            self.writeCommand("close")

    def MXElectrixHandle(self):
        print(common_utils.getTimeString(), "INFO: A new TCP connection from " + self.client_address[0])

        currentLine = self.readLine()

        # the new way of handshaking
        cnt = 0
        while currentLine.find("connect2") == -1:
            cnt = cnt + 1
            if cnt > 10:
                # A stupid way to realize that the connection is from someone else or the stream is going on
                print(common_utils.getTimeString(), "ERROR: Did not get expected handshake from",
                      self.client_address[0])
                self.writeCommand("esc")
                self.writeCommand("close")
                return
            currentLine = self.readLine()

        # Open the connection by sending "open"-command
        print(common_utils.getTimeString(), "INFO: Opening connection to", self.client_address[0])
        self.writeCommand("open")

        # Start the stream by sending "get_stream, rations"-command
        print(common_utils.getTimeString(), "INFO: Starting the stream from", self.client_address[0])
        self.writeCommand("get_stream, ratios")

        stream_on = False
        format_set = False
        module_id = ""
        streamFormat = []

        cnt = 0
        while True:
            currentLine = self.readLine()
            # get the timestamp immediatly after reading the input line
            tm = int(round(time.time() * 1000))
            if not currentLine:
                # received an empty line
                continue

            currentLineType = mxelectrix_parser.getLineType(currentLine)
            if currentLineType == "header":
                # if get header, check that moduleID has been set and start stream if type of get_stream
                if module_id == "":
                    module_id = mxelectrix_parser.parseModuleId(currentLine)
                    print(common_utils.getTimeString(), "INFO: Module ID set as " + module_id, "for",
                          self.client_address[0])

                headerType = mxelectrix_parser.getHeaderType(currentLine)
                if headerType == "get_stream":
                    print(common_utils.getTimeString(), "INFO: Data stream is on for", self.client_address[0])
                    stream_on = True
                    continue

            if stream_on and not format_set and currentLineType == "format":
                # stream is on, but not format yet and we got format -> should be the format we are looking for
                streamFormat = mxelectrix_parser.getStreamFormat(currentLine)
                format_set = True
                print(common_utils.getTimeString(), "INFO: Data format is set for", self.client_address[0])
                continue

            if stream_on and format_set and currentLineType == "data":
                # line should be data in this situation!
                data = mxelectrix_parser.parseMeasurement(currentLine, streamFormat)
                if not data:
                    print(common_utils.getTimeString(), "ERROR: Did not receive data as predicted from",
                          self.client_address[0])
                    return
                else:
                    # send data to ProCem RTL server
                    # slice the date and time away from the data
                    sendDataToProcem(module_id, data[2:], tm)

                    # NOTE: comment the following out if you don't want periodical messages about sent packages
                    cnt += 1
                    if cnt % 3600 == 0:
                        print(common_utils.getTimeString(), "INFO:", self.client_address[0],
                              "has sent", cnt, "packages")


def sendDataToProcem(module_id, data, timestamp):
    # data is tuple of (meta, measurement), where meta is a tuple (value, unit)

    for item in data:
        value = item[0][0]
        unit = item[0][1]
        measurement = float(item[1])

        measurementInfo = dataModel.getMeasurementInfo(module_id, value)
        if measurementInfo is None:
            print(common_utils.getTimeString(), "WARN: measurement", value, "is not configured for", module_id)
            continue

        name = measurementInfo.name
        path = measurementInfo.path
        rtl_id = measurementInfo.rtl_id
        confidential = not measurementInfo.ticket

        # this will be added to the current payload
        newPkt = bytes(common_utils.getProcemRTLpkt(
            name, path, measurement, timestamp, unit, "double", rtl_id, confidential), "utf-8")

        # put the new packet in the global data queue
        # a worker in a separate thread handles the actual data sending to procem
        data_queue.put(newPkt)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main_config_file = sys.argv[1]
    else:
        print("Start this adapter with 'python3", sys.argv[0], "<configuration file>' command")
        quit()

    print(common_utils.getTimeString(), "Reading the configurations")
    # read configuration information from the configuration files
    try:
        main_config = common_utils.readConfig(main_config_file)
        MXELECTRIX_RECEIVER_IP = main_config["mxelectrix_receiver_ip"]
        MXELECTRIX_RECEIVER_PORT = main_config["mxelectrix_receiver_port"]
        CONFIG_SCHEME_FILE_NAME = main_config.get("model_config_file", CONFIG_SCHEME_FILE_NAME)
        MEASUREMENT_ID_FILE_NAME = main_config.get("measurement_id_file", MEASUREMENT_ID_FILE_NAME)

        dataModel = mxelectrix_model.load_model(
            csvFilename=MEASUREMENT_ID_FILE_NAME, configFilename=CONFIG_SCHEME_FILE_NAME)
    except Exception as error:
        print(common_utils.getTimeString(), "ERROR:", error)
        quit()

    threading.Thread(target=common_utils.procemSendWorker, kwargs={"data_queue": data_queue}).start()

    print(common_utils.getTimeString(), "Starting the server for MXElectrix")
    connection_try_interval = 0.0
    connection_try_increase = 5.0
    server = None
    while server is None:
        try:
            server = ThreadedTCPServer((MXELECTRIX_RECEIVER_IP, MXELECTRIX_RECEIVER_PORT), MXElectrixTCPHandler)
        except:
            connection_try_interval += connection_try_increase
            print(common_utils.getTimeString(), "Connection to server failed. Trying again in",
                  connection_try_interval, "seconds")
            time.sleep(connection_try_interval)

    try:
        server.serve_forever()
    except:
        print()
        print(common_utils.getTimeString(), "Closing the server")
    finally:
        # Clean-up server (close socket, etc.)
        data_queue.put(None)
        server.shutdown()
        server.server_close()
