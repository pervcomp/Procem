# -*- coding: utf-8 -*-
"""This module contains a functions to read locally stored Procem data and to send it to IoT-Ticket."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import csv
import datetime
import math
import pathlib
import queue
import random
import subprocess
import sys
import threading
import time

import adapters.common_utils as common_utils
import iotticket_utils


def remove_file(filename):
    """Removes the given file from the file system."""
    try:
        removal = subprocess.run(["rm", "-f", str(filename)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return removal.returncode == 0
    except Exception as error:
        print(common_utils.getTimeString(), "Remove file:", error)
        return False


class IoTTicketInfo:
    """Class for holding the needed information about the measurements."""
    def __init__(self, conf_file, delimiter=";"):
        self.__conf_file = conf_file
        self.__delimiter = delimiter
        self.__read_info()

    def get_packet(self, rtl_id, value, timestamp, ignore_confidential=False):
        """Takes in the id number, measurement value and timestamp, and returns a IoT-Ticket-compatible json object."""
        if rtl_id not in self.__info:
            return None
        id_info = self.__info[rtl_id]
        if id_info["confidential"] and not ignore_confidential:
            # no json object is created if the measurement is confidential
            return None

        name = id_info["name"]
        path = id_info["path"]
        unit = id_info["unit"]
        datatype = id_info["datatype"]

        if datatype == "double":
            value = float(value)
        elif datatype == "long":
            value = int(value)
        elif datatype == "boolean":
            value = bool(value)
        elif datatype == "string":
            value = str(value)

        return {
            "name": name,
            "path": path,
            "v": value,
            "ts": timestamp,
            "unit": unit,
            "type": datatype
        }

    def __read_info(self):
        """Reads the measurement information from a CSV file."""
        self.__info = {}
        with open(self.__conf_file, "r") as file:
            reader = csv.DictReader(file, delimiter=self.__delimiter)
            for row in reader:
                try:
                    self.__info[int(row["rtl_id"])] = {
                        "name": row["name"],
                        "path": row["path"],
                        "unit": row["unit"],
                        "datatype": row["datatype"],
                        "confidential": row["confidential"] != "" or row["iot_ticket"] == ""
                    }
                except ValueError:
                    continue
                except KeyError:
                    continue


class IoTTicketSender:
    """Class for sending the data from local storage to IoT-Ticket.
       It is advisable to always use the stop() function before exiting the program."""
    def __init__(self, conf_file):
        self.__failure = True  # this will be True unless the sender is setup properly
        config = common_utils.readConfig(conf_file)
        if config is None:
            return

        id_conf_file = config["id-configuration-file"]
        id_delimiter = config.get("id-delimiter", ";")
        self.__info = IoTTicketInfo(id_conf_file, id_delimiter)

        username = config["username"]
        password = config["password"]
        base_url = config["base-url"]
        self.__device_id = config["deviceid"]
        self.__client = iotticket_utils.SimpleIoTTicketClient(base_url, username, password)

        self.__data_folder = pathlib.Path(config["data-folder"])
        self.__remote_server = config.get("remote-server", None)
        self.__remote_folder = pathlib.Path(config.get("remote-folder", ""))
        self.__filename_format = config["data-filename-format"]
        self.__filename_suffix = config["data-filename-suffix"]
        self.__compressed_suffix = config["data-compressed-suffix"]
        self.__data_delimiter = config.get("data-delimiter", "\t")
        queue_size = config.get("iotticket-queue-size", 100000)
        self.__buffer_size = config.get("iotticket-buffer-size", queue_size)
        self.__data_queue = queue.Queue(maxsize=queue_size)

        self.__packet_size = config.get("iotticket-max-packet-size", 500)
        self.__delay = config.get("iotticket-minimum-delay-s", 1.0)
        self.__max_retries = config.get("iotticket-maximum-retries", 5)

        self.__last_send = time.time()
        self.__queue_thread = threading.Thread(target=self.__send_data_from_queue)
        self.__queue_thread.start()
        self.__failure = False

    def stop(self):
        """Stops the sender and waits for any ongoing IoT-Ticket sends to be finished."""
        if not self.__failure:
            self.__data_queue.put(None)
            self.__failure = True
            self.__queue_thread.join()

    def __del__(self):
        self.stop()

    def send(self, start_date, end_date=None):
        """Sends the data from the local storage to the IoT-Ticket. All data between start_date and end_date will
           be send. If end_date is None, then only the data corresponding start_date will be send."""
        if self.__failure:
            print("Sender is not setup properly.")
            return

        if end_date is None:
            end_date = start_date
        date = start_date
        while date <= end_date:
            base_name = self.__filename_format.format(year=date.year, month=date.month, day=date.day)
            filename = self.__data_folder / (base_name + self.__filename_suffix)
            filename_compressed = filename.with_suffix(self.__compressed_suffix)
            data_exists = filename.is_file()
            remove_flag = False
            remove_compressed_flag = False
            if not data_exists:
                # if the uncompressed data file was not found, try to find the compressed file and uncompress it
                if filename_compressed.is_file() and self.__uncompress_file(filename_compressed):
                    remove_flag = True
                else:
                    # try to copy the compressed data file from remote server to the local data folder
                    remote_file = self.__remote_folder / (base_name + self.__compressed_suffix)
                    if self.__copy_from_remote(remote_file):
                        # uncompress the data file copied from the remote server
                        if filename_compressed.is_file() and self.__uncompress_file(filename_compressed):
                            remove_flag = True
                            remove_compressed_flag = True

            if filename.is_file():
                self.__read_file(filename)
            else:
                print("ERROR: Could not read file:", filename)

            if remove_flag and filename_compressed.is_file():
                # remove the uncompressed file if it was uncompressed for reading
                remove_file(filename)
            if remove_compressed_flag:
                # remove the compressed file if it was copied from the remote server
                remove_file(filename_compressed)

            date += datetime.timedelta(days=1)

        self.__data_queue.put("")

    def __read_file(self, filename):
        """Reads a data file and sends the data to the data queue."""
        try:
            with open(str(filename), "r") as file:
                print(common_utils.getTimeString(), "Reading file", filename)
                for row in file:
                    items = row.strip().split(self.__data_delimiter)
                    if len(items) == 3:
                        try:
                            rtl_id = int(items[0])
                            value = items[1]
                            ts = int(items[2])
                            self.__data_queue.put((rtl_id, value, ts))
                        except ValueError:
                            continue
        except OSError as error:
            print(common_utils.getTimeString(), "Read file:", error)

    def __copy_from_remote(self, filename):
        """Copies a file from a remote server to the local data folder."""
        try:
            remote_path = pathlib.Path(":".join([self.__remote_server, str(self.__remote_folder)]))
            full_filename = str(remote_path / filename.name)
            command = ["scp", full_filename, str(self.__data_folder)]
            copying = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return copying.returncode == 0
        except Exception as error:
            print(common_utils.getTimeString(), "Copy:", error)
            return False

    def __uncompress_file(self, filename):
        """Uncompress the given file using 7z. Returns True if the operation was successful."""
        try:
            command = ["7z", "e", "-y", "-o" + str(self.__data_folder), str(filename)]
            uncompress = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return uncompress.returncode == 0
        except Exception as error:
            print(common_utils.getTimeString(), "Uncompress:", error)
            return False

    def __send_data_from_queue(self):
        """Reads values from the data queue and sends them to the IoT-Ticket using a buffer."""
        buffer = []
        while True:
            item = self.__data_queue.get()
            if item is None or item == "":
                if len(buffer) > 0:
                    self.__send_buffer_to_iotticket(buffer)
                if item is None:
                    return
                buffer = []

            try:
                rtl_id, value, timestamp = item
                packet = self.__info.get_packet(rtl_id, value, timestamp)
                if packet is None:
                    continue
                buffer.append(packet)

                if len(buffer) >= self.__buffer_size:
                    self.__send_buffer_to_iotticket(buffer)
                    buffer = []
            except ValueError:
                continue

    def __send_buffer_to_iotticket(self, buffer):
        """Sends the data contained in the buffer to the IoT-Ticket."""
        time.sleep(max(0, self.__delay - (time.time() - self.__last_send)))
        self.__last_send = time.time()

        # sort the data buffer in the hopes of speeding up the transfer
        buffer.sort(key=lambda x: (x["path"], x["name"], x["ts"]))

        start_time = time.time()
        in_progress = True
        try_number = 1
        n_packets = int(math.ceil(len(buffer) / self.__packet_size))
        extra_wait = 0  # this can be set to higher value if extra wait time between retries is needed
        confirmed_written = 0
        considered_packets = set(range(n_packets))
        while in_progress and try_number <= self.__max_retries:
            # if this is not the first try, sleep for a few seconds before trying again
            if try_number > 1:
                time.sleep(try_number * self.__delay * (random.uniform(1.0, 2.0) + extra_wait))

            try:
                responces = self.__client.writeData(self.__device_id, buffer, self.__packet_size, considered_packets)
                (total_written, extra_wait_check) = iotticket_utils.getResponceInfo(
                    responces=responces,
                    n_measurements=len(buffer),
                    packet_size=self.__packet_size,
                    considered_packets=considered_packets)

                confirmed_written += total_written
                if extra_wait_check:
                    extra_wait = 2.0
                else:
                    extra_wait = 0

                # check if some packets need to be resend
                if len(considered_packets) > 0:
                    try_number += 1
                else:
                    in_progress = False

            except Exception as error:
                responces = getattr(error, "responces", [None] * n_packets)
                (total_written, extra_wait_check) = iotticket_utils.getResponceInfo(
                    responces=responces,
                    n_measurements=len(buffer),
                    packet_size=self.__packet_size,
                    considered_packets=considered_packets)
                confirmed_written += total_written
                extra_wait = 3.0  # wait a bit of extra time before resend because there was a connection failure
                try_number += 1

        time_diff = round(time.time() - start_time, 1)
        print(common_utils.getTimeString(), " IoTT: ", confirmed_written, "/", len(buffer), ", ", sep="", end="")
        if try_number == 1:
            print(try_number, "try", end="")
        else:
            print(try_number, "tries", end="")
        print(", Time:", time_diff, "seconds")


def print_usage_info():
    """Prints usage information."""
    print("Usage: python3", sys.argv[0], "conf.json start_date [end_date]")
    print("  date format: {year}-{month}-{day} e.g. 2018-7-1 or 2018-07-01")


if __name__ == "__main__":
    try:
        configuration_file = sys.argv[1]
        start_list = [int(value) for value in sys.argv[2].split("-")]
        start = datetime.date(year=start_list[0], month=start_list[1], day=start_list[2])
        if len(sys.argv) == 4:
            end_list = [int(value) for value in sys.argv[3].split("-")]
            end = datetime.date(year=end_list[0], month=end_list[1], day=end_list[2])
        else:
            end = None

        sender = IoTTicketSender(configuration_file)
        sender.send(start, end)
        sender.stop()

    except IndexError as error:
        print_usage_info()
        print(error)
    except ValueError as error:
        print_usage_info()
        print(error)
