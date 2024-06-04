# -*- coding: utf-8 -*-
"""This module contains helper functions and classes for Procem RTL and the adapters."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import datetime
import json
import os
import pathlib
import queue
import re
import socket
import socketserver
import threading
import time

# the address of the Procem RTL UDP server
PROCEM_SERVER_IP = "127.0.0.1"
PROCEM_SERVER_PORT = 6666

# the format for the UDP packets
PROCEM_RTL_PKT_FLOAT = '{{"name":"{0}","path":"{1}","v":{2:f},"ts":{3:d},"unit":"{4}",' + \
                       '"type":"double","id":{5:d},"secret":{6}}}\n'
PROCEM_RTL_PKT_LONG = '{{"name":"{0}","path":"{1}","v":{2:d},"ts":{3:d},"unit":"{4}",' + \
                      '"type":"long","id":{5:d},"secret":{6}}}\n'
PROCEM_RTL_PKT_BOOL = '{{"name":"{0}","path":"{1}","v":{2},"ts":{3},"unit":"{4}",' + \
                      '"type":"boolean","id":{5:d},"secret":{6}}}\n'

# maximum size for UDP payload. Current value based on a quick experiment where it was 8192
UDP_MAX_SIZE = 8000

# the minimum time interval between udp sends (used in procemSendWorker)
MIN_UDP_INTERVAL = 0.01

# these settings are used to setup resends for udp packets
USE_UDP_CONFIRMATION = True
MAX_UDP_RESENDS = 4
SOCKET_TIMEOUT = 0.5
CONFIRMATION_MESSAGE = bytes("OK", "utf-8")
CONFIRMATION_LENGTH = len(CONFIRMATION_MESSAGE)

# these settings are used to setup a backup file for holding failed udp sends
USE_FILE_BACKUP = True
BACKUP_WRITE_FILENAME = "failed_udp_data_sends.txt"
BACKUP_READ_FILENAME = "failed_udp_data_sends_temp.txt"
BACKUP_QUEUE_SIZE = 4096

# these are used when querying the most resent value of some variable
GET_VALUE_MESSAGE = bytes("get_value:", "utf-8")
GET_VALUE_LENGTH = len(GET_VALUE_MESSAGE)
VALUE_SEPARATION_CHAR = ";"


def usleep(x):
    """Sleeps for x microseconds."""
    time.sleep(x/1000000.0)


def chunks(big_list, n):
    """Yield successive n-sized chunks from big_list. By Ned Batchelder at
       https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks"""
    for i in range(0, len(big_list), n):
        yield big_list[i:i + n]


def getProcemRTLpkt(name, path, value, timestamp, unit, datatype="double",
                    variableNumber=-1, confidential=False):
    """Creates a json data packet that Procem RTL expects"""
    if type(confidential) is bool:
        confidential = str(confidential).lower()
    else:
        raise ValueError("Confidential should be boolean but was {}.".format(confidential))

    if datatype == "double" or datatype == "float":
        return PROCEM_RTL_PKT_FLOAT.format(name, path, float(value), timestamp, unit, variableNumber, confidential)
    elif datatype == "long" or datatype == "int" or datatype == "integer":
        return PROCEM_RTL_PKT_LONG.format(name, path, int(value), timestamp, unit, variableNumber, confidential)
    elif datatype == "bool" or datatype == "boolean":
        return PROCEM_RTL_PKT_BOOL.format(
            name, path, str(bool(value)).lower(), timestamp, unit, variableNumber, confidential)
    else:
        raise ValueError("ERROR: unsupported data type {} - TODO: go fix this?".format(datatype))


def getValidatedRTLpkts(rtl_pkts):
    """Returns a list of validated json objects."""
    validated_data = []
    rtl_pkt_rows = rtl_pkts.strip().decode("utf-8").splitlines()

    for pkt in rtl_pkt_rows:
        try:
            data = json.loads(pkt)
            name = data["name"]
            path = data["path"]
            v = data["v"]
            ts = data["ts"]
            unit = data["unit"]
            datatype = data["type"]
            rtl_id = data["id"]
            secret = data.get("secret", False)
            if (type(name) == str and 0 < len(name) <= 100 and
                    type(path) == str and len(path) <= 1000 and re.match("(\\/[a-zA-Z0-9]+){1,10}$", path) and
                    ((type(v) == float and datatype == "double") or
                        (type(v) == int and datatype == "long") or
                        (type(v) == bool and datatype == "boolean")) and
                    type(ts) == int and
                    type(unit) == str and len(unit) <= 10 and
                    type(datatype) == str and (datatype == "double" or datatype == "long" or datatype == "boolean") and
                    type(rtl_id) == int and
                    type(secret) == bool):
                validated_data.append(data)
            else:
                print("failed data:", data)
        except Exception as error:
            print(error, pkt)

    if len(rtl_pkt_rows) != len(validated_data):
        print("WARNING:", len(validated_data), "out of", len(rtl_pkt_rows), "packets validated")
    return validated_data


def readConfig(fileName):
    """Reads the JSON based configuration file and returns its contents as a dictionary."""
    try:
        with open(fileName, 'r') as configFile:
            configuration = json.load(configFile)
            return configuration

    except FileNotFoundError:
        print("Configuration file {} not found.".format(fileName))
        return None

    except json.decoder.JSONDecodeError as error:
        print("Unable to parse JSON from configuration file: {}.".format(str(error)))
        return None


def startUDPserver(handler, ip=PROCEM_SERVER_IP, port=PROCEM_SERVER_PORT):
    """Starts and returns a threaded UDP server."""
    server = socketserver.ThreadingUDPServer((ip, port), handler)

    server_thread = threading.Thread(target=server.serve_forever)
    server.max_packet_size = 8192 * 4
    server_thread.daemon = True
    server_thread.start()
    return server


def stopUDPserver(server):
    """Shuts down the given server."""
    server.shutdown()


def receiveConfirmation(sock):
    """Try to receive a confirmation message from the socket."""
    try:
        confirmation = sock.recv(CONFIRMATION_LENGTH)
    except socket.timeout:
        confirmation = ""
    except Exception as error:
        print(error)
        confirmation = ""

    return confirmation


def sendUDPmsg(dst_ip, dst_port, src_ip="", src_port=0, message=bytes(), use_confirmation=USE_UDP_CONFIRMATION):
    """Sends a UDP message to given destination from given source"""
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(SOCKET_TIMEOUT)
        if src_ip != "":
            sock.bind((src_ip, src_port))
        sock.sendto(message, (dst_ip, dst_port))

        if use_confirmation:
            confirmation = receiveConfirmation(sock)

            resend_try = 1
            while confirmation != CONFIRMATION_MESSAGE and resend_try <= MAX_UDP_RESENDS:
                sock.sendto(message, (dst_ip, dst_port))
                confirmation = receiveConfirmation(sock)
                resend_try += 1

            if confirmation != CONFIRMATION_MESSAGE:
                print(getTimeString(), "ERROR: UDP send failed after", resend_try, "tries.")
                if USE_FILE_BACKUP:
                    BackupFileHandler.writeData(message)

    finally:
        if sock is not None:
            sock.close()


def sendUDPmsgWithSocket(dst_ip, dst_port, socket, message=bytes()):
    """Sends a UDP message to given destination using the given socket"""
    socket.sendto(message, (dst_ip, dst_port))


def getPresentValue(rtl_id):
    """Asks procem_rtl for the present value of a variable and returns the result."""
    sock = None
    try:
        message = GET_VALUE_MESSAGE + bytes(str(rtl_id), "utf-8")

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message, (PROCEM_SERVER_IP, PROCEM_SERVER_PORT))

        sock.settimeout(SOCKET_TIMEOUT)
        resp = sock.recv(UDP_MAX_SIZE)

        values = resp.strip().decode("utf-8").split(VALUE_SEPARATION_CHAR)
        if len(values) != 3:
            return (None,) * 3

        rtl_id = int(values[0])
        v = values[1]
        ts = int(values[2])

    except:
        return (None,) * 3
    finally:
        if sock is not None:
            sock.close()

    return rtl_id, v, ts


def procemSendWorker(data_queue, clear_item=bytes()):
    """Sends data to procem in buffered packets using the given queue."""
    buffer = bytes()
    last_send_time = time.time()
    while True:
        packet = data_queue.get()
        if packet is None:
            if len(buffer) > 0:
                threadedProcemSend(buffer)
            break
        elif packet == clear_item:
            if len(buffer) > 0:
                threadedProcemSend(buffer)
                buffer = bytes()
            continue

        if len(buffer) + len(packet) > UDP_MAX_SIZE:
            # to avoid too quick sending ensure that at least the minimum time has passed since last send
            sleep_time = MIN_UDP_INTERVAL - (time.time() - last_send_time)
            time.sleep(max(0.0, sleep_time))

            threadedProcemSend(buffer)
            last_send_time = time.time()
            buffer = packet
        else:
            buffer += packet


def threadedProcemSend(data):
    """Creates a new thread and sends the data as a UDP packet to procem using it."""
    threading.Thread(
        target=sendUDPmsg,
        kwargs={
            "dst_ip": PROCEM_SERVER_IP,
            "dst_port": PROCEM_SERVER_PORT,
            "message": data
        }).start()


def getTimeString():
    """Returns the current time as a string."""
    ts = time.localtime()
    return "({:02d}.{:02d}.{:04d} {:02d}:{:02d}:{:02d})".format(
        ts.tm_mday, ts.tm_mon, ts.tm_year, ts.tm_hour, ts.tm_min, ts.tm_sec)


class Counter:
    """Counter class implements a threadsafe counter that can be incremented and decreased."""
    def __init__(self, initial=0):
        self.__value = initial
        self.__lock = threading.Lock()

    def increase(self, value=1):
        with self.__lock:
            self.__value += value

    def decrease(self, value=1):
        with self.__lock:
            self.__value -= value

    def getValue(self):
        with self.__lock:
            return self.__value


class BackupFileHandler:
    """Class for handling the writing to and reading from the backup file."""
    __read_lock = threading.Lock()
    __write_lock = threading.Lock()
    __read_filename = BACKUP_READ_FILENAME
    __write_filename = BACKUP_WRITE_FILENAME

    @classmethod
    def changeDirectory(cls, directory):
        """Change the base directory of the used backup file names."""
        read_file_path = pathlib.PurePath(BACKUP_READ_FILENAME)
        write_file_path = pathlib.PurePath(BACKUP_WRITE_FILENAME)
        new_path = pathlib.PurePath(directory)
        cls.__read_filename = str(new_path / read_file_path)
        cls.__write_filename = str(new_path / write_file_path)

    @classmethod
    def writeData(cls, data):
        """Write data to the backup file."""
        with cls.__write_lock:
            try:
                with open(cls.__write_filename, "a") as file:
                    file.write(data.decode("utf-8"))
            except OSError:
                print(getTimeString(), "cannot write to", cls.__write_filename)

    @classmethod
    def readData(cls):
        """Read data from the backup file, send it to ProCem RTL and delete the file backup.
           Return the number of read data lines."""
        with cls.__read_lock:
            # create a new queue for sending the data to procem_rtl
            data_queue = queue.Queue(BACKUP_QUEUE_SIZE)
            threading.Thread(target=procemSendWorker, kwargs={"data_queue": data_queue}).start()

            lines = 0
            try:
                # read the data from previous read file (it might remain if the program was stopped abruptly)
                if os.path.isfile(cls.__read_filename):
                    with open(cls.__read_filename, "r") as file:
                        for row in file:
                            item = bytes(row, "utf-8")
                            data_queue.put(item)
                            lines += 1
            except OSError:
                print(getTimeString(), "cannot read from", cls.__read_filename)

            with cls.__write_lock:
                # rename the backup file so new failed data sends will be written to a new file
                try:
                    if os.path.isfile(cls.__write_filename):
                        os.rename(cls.__write_filename, cls.__read_filename)
                except OSError:
                    print(getTimeString(), "cannot rename", cls.__write_filename, "to", cls.__read_filename)

            try:
                if os.path.isfile(cls.__read_filename):
                    with open(cls.__read_filename, "r") as file:
                        for row in file:
                            item = bytes(row, "utf-8")
                            data_queue.put(item)
                            lines += 1
            except OSError:
                print(getTimeString(), "cannot read from", cls.__read_filename)

            # delete the temporary backup file since all items have been handled
            try:
                if os.path.isfile(cls.__read_filename):
                    os.remove(cls.__read_filename)
            except OSError:
                print(getTimeString(), "cannot remove", cls.__read_filename)

            data_queue.put(None)

        return lines


def to_iso_format_datetime_string(unix_timestamp_ms: int) -> str:
    """Returns the given datetime value as ISO 8601 formatted string in UTC timezone."""
    dt = datetime.datetime.fromtimestamp(unix_timestamp_ms / 1000, datetime.timezone.utc)
    return (
        f"{dt.year:04}-{dt.month:02}-{dt.day:02}" +
        f"T{dt.hour:02}:{dt.minute:02}:{dt.second:02}.{(dt.microsecond // 1000):03}Z"
    )
