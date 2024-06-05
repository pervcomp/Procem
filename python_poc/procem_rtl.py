# -*- coding: utf-8 -*-
"""This module contains the main code for Procem RTL workers."""

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
import queue  # Queue is "defacto" communication FIFO, LIFO or priority queue for inter-thread communication
import math
import random
import socketserver
import sys
import threading
import time

import adapters.common_utils as common_utils
import utils.datastorage as datastorage
import iotticket_utils
import tcp_utils


# deque is fast O(1) queue for thread safe a FIFO/LIFO/FILO implementations
# append(), appendleft(), pop(), popleft(),
# from collections import deque

# use the common file to setup the UDP server ip and port
PROCEM_SERVER_IP = common_utils.PROCEM_SERVER_IP
PROCEM_SERVER_PORT = common_utils.PROCEM_SERVER_PORT

# All the default parameters below can be changed using the configuration file.
PROCEM_CONF_FILE = "myconf.txt"

# These determine whether the received data is stored in the local storage and
# whether the data is sent to IoT-Ticket as well as the battery demo program
DB_STORAGE_CHECK = True
IOTTICKET_SEND_CHECK = True
BATTERY_DEMO_CHECK = False

# How many items the IOTTicketBufferingWorker buffers before sending to IoTTicket
# This value should be selected carefully so that buffering does not happen over long periods of time
# as it may mess up the dashboard timestamps
# NOTE: currently this is the minimum buffer, it will be increased
#       if the IoT-Ticket workers cannot otherwise handle the data flow.
IOTTICKET_WRITING_BUFFER = 50

# The maximum size of a single packet send to IoT-Ticket (in the number of measurements)
IOTTICKET_MAX_PACKET_SIZE = 500

# The minimum delay in seconds between data sending connections to the IoT-Ticket
IOTTICKET_MINIMUM_DELAY = 1.0

# The maximum number of tries that are made when sending data packet to the IoT-Ticket
IOTTICKET_MAXIMUM_RETRIES = 5

# How many IoTTicket workers are used. This should reduce the bottleneck to the IoTTicket HTTP-communication
# NOTE: currently this is the maximum number of the worker threads.
# This should not be too large to avoid bombarding the IoT-Ticket with multiple simultaneous connections.
PROCEM_IOTTICKET_WORKERS = 10

# The maximum number times a data can be cycled after failed IoT-Ticket sends.
IOTTICKET_MAX_DATA_CYCLES = 5

# How many measurements are packed together when cycling data after failed IoT-Ticket send.
IOTTICKET_CYCLE_BUFFER_SIZE = 10

# The queue sizes for the IoT-Ticket and the local data storage.
# These could be infinite, but it is good idea to set maximum size to prevent exhaustion in a case of malfunction etc.
IOTTICKET_QUEUE_SIZE = 1024
DB_QUEUE_SIZE = 1024

# Queues for worker threads
# actually created in the main part
PROCEM_MAIN_QUEUE = None
PROCEM_DB_QUEUE = None
PROCEM_IOTTICKET_QUEUE = None
PROCEM_VALUE_QUERY_QUEUE = None

# global to carry deviceID and user credential over whole procem rtl
PROCEM_DEVICEID = ""
PROCEM_USERNAME = ""
PROCEM_PASSWORD = ""
PROCEM_BASEURL = ""
IOTTICKET_DEVICES = {}
IOTTICKET_VERSION = "old"

# the rtl ids that the battery demo program is interested in
IDS_FOR_BATTERY = set()

# Set this to True if you want a faster exit when quitting
DAEMON_THREAD_WORKERS = False

# The number of the most recent values saved in the present value table
PRESENT_VALUE_COUNT = 1
# A dictionary that holds the present values of the measurements, created in the main part
PRESENT_VALUES = None

# TODO: at least some if not all of these global constanst/variables could be changed to local variables


class ReceivedUDPHandler(socketserver.BaseRequestHandler):
    """A callback handler to handle incoming UDP packets (that Procem UDP server receives)."""
    def handle(self):
        data = self.request[0]

        # check if the message is a query for a present value of a variable
        if data[:common_utils.GET_VALUE_LENGTH] == common_utils.GET_VALUE_MESSAGE:
            PROCEM_VALUE_QUERY_QUEUE.put((data[common_utils.GET_VALUE_LENGTH:], self.request[1], self.client_address))
            return

        # To minimize the time spent in this function, just put the data to the main queue and let
        # the validation worker handle the data validation and adding the data to the worker item queues.
        PROCEM_MAIN_QUEUE.put(data)
        # TODO: Figure out is there any other way to give the queue to UDPHandlers than global
        # TODO: Data should be combined with identifier of the device.
        # TODO: The question remain: what should be on shoulders of the adapter and what on Procem
        # NOTE: There is a way to avoid using global queues. See tcp_utils.py for an example.

        if common_utils.USE_UDP_CONFIRMATION:
            socket = self.request[1]
            socket.sendto(common_utils.CONFIRMATION_MESSAGE, self.client_address)


def validationWorker():
    """Validates the received data and sends the validated_data to the other worker queues."""
    global DB_STORAGE_CHECK
    global IOTTICKET_SEND_CHECK
    global BATTERY_DEMO_CHECK
    global IDS_FOR_BATTERY

    while True:
        data = PROCEM_MAIN_QUEUE.get()
        if data is None:
            PROCEM_DB_QUEUE.put(None)
            PROCEM_IOTTICKET_QUEUE.put(None)
            PROCEM_VALUE_QUERY_QUEUE.put(None)
            PROCEM_BATTERY_QUEUE.put(None)
            break

        # validate the received data
        validated_data = common_utils.getValidatedRTLpkts(data)

        # send the validated data to the other worker threads
        if DB_STORAGE_CHECK:
            PROCEM_DB_QUEUE.put(getProcemQueueItem(validated_data))

        if IOTTICKET_SEND_CHECK:
            # give the data as (item, cycle_number) tuples to the IoT-Ticker sender
            PROCEM_IOTTICKET_QUEUE.put(getProcemQueueItem([(item, 0) for item in validated_data]))

        # update the present value table
        for item in validated_data:
            rtl_id = item.get("id", None)
            v = item.get("v", None)
            ts = item.get("ts", None)
            if id is not None and v is not None and ts is not None:
                PRESENT_VALUES.add_value(rtl_id=rtl_id, value=v, timestamp=ts)

                if BATTERY_DEMO_CHECK and rtl_id in IDS_FOR_BATTERY:
                    PROCEM_BATTERY_QUEUE.put(item)


def presentValueQueryWorker():
    """Handles the sending of the present values for queries that asks for them."""
    separation_char = common_utils.VALUE_SEPARATION_CHAR
    while True:
        query = PROCEM_VALUE_QUERY_QUEUE.get()
        if query is None:
            break
        if len(query) != 3:
            continue

        try:
            rtl_id = int(query[0].strip().decode("utf-8"))
            socket = query[1]
            address = query[2]

            values = PRESENT_VALUES.get_values(rtl_id)
            if values is None:
                responce = ""
            else:
                values_str = [separation_char.join([str(value.v), str(value.ts)]) for value in values]
                responce = separation_char.join([str(rtl_id)] + values_str)

            socket.sendto(bytes(responce, "utf-8"), address)

        except:
            pass


def batteryWorker():
    """Handles sending data to the battery demo program using a TCP connection."""
    client = tcp_utils.TCPClient(tcp_utils.BATTERY_SERVER_IP, tcp_utils.BATTERY_SERVER_PORT)

    MAX_FAILS = 10  # ignore is set if this many consequent sends fail
    FAIL_INTERVAL = 600.0  # ignore is released after this many seconds
    fail_count = 0
    check_time = time.time()

    while True:
        item = PROCEM_BATTERY_QUEUE.get()
        if item is None:
            break
        elif fail_count > MAX_FAILS:
            if time.time() < check_time:
                continue
            else:
                fail_count = 0
                print(common_utils.getTimeString(), "Procem RTL, Ignore OFF for battery worker")

        try:
            data = {
                "id": item["id"],
                "v": item["v"],
                "ts": item["ts"]
            }
            success = client.robustSend(json.dumps(data))
            if not success:
                PROCEM_BATTERY_QUEUE.put(item)
                fail_count += 1
                if fail_count > MAX_FAILS:
                    check_time = time.time() + FAIL_INTERVAL
                    print(common_utils.getTimeString(), "Procem RTL, Ignore ON for battery worker")
            else:
                fail_count = 0
        except:
            pass

    client.close()


def getProcemQueueItem(data):
    """Returns a worker queue item from the data."""
    # NOTE: currently the same device id is used for all the data. Changes are needed
    # also to procemIOTTicketWorker if support for several device ids is considered
    # NOTE: Opening several different instances of procem_rtl can be used for handling several different devices.
    # This would also require some extra configurations for the UDP server port numbers.
    return {
        "device_id": PROCEM_DEVICEID,
        "data": data
    }


def procemDBWorker():
    """Saves measurements in a log file in the same format as they were received."""
    day = datetime.date.today().isoformat()
    logfile = open(day + "_procem.log", "a")
    while True:
        items = PROCEM_DB_QUEUE.get()
        if items is None:
            # Bail out?
            break

        # check log file date, and update if day has changed
        if day != datetime.date.today().isoformat():
            logfile.close()
            day = datetime.date.today().isoformat()
            logfile = open(day + "_procem.log", "a")

        # write entry in DB or file..
        logfile.write(str(items["data"]) + "\n")

        PROCEM_DB_QUEUE.task_done()

    logfile.close()


def procemCSVLogWorker():
    """Saves measurements into a CSV file. Variable number, value and timestamp will be saved.
       A new csv file is created for each day."""
    # separator for items in a line
    DELIMITER = '\t'
    # file name is the date and the name variable
    name = "_procem.csv"
    # data counter file to more easily keep track of the number of measurements for each id.
    counter_name = "_data_counter.csv"
    day = datetime.date.today().isoformat()
    logfile = open(day + name, "a")
    data_counter = {}
    item_count = 0

    while True:
        items = PROCEM_DB_QUEUE.get()
        if items is None:
            # Bail out.
            break

        # check log file date, and update if day has changed
        if day != datetime.date.today().isoformat():
            logfile.close()
            writeDataCounterToFile(day + counter_name, data_counter)
            day = datetime.date.today().isoformat()
            logfile = open(day + name, "a")
            data_counter = {}

        # string to be written to the file
        data = ""
        # items should be a list of json objects each containing a single measurement
        for item in items["data"]:
            try:
                number = item["id"]
                value = item["v"]
                timeStamp = item["ts"]

                if number is not None and value is not None and timeStamp is not None:
                    # keep a counter on how many times each measurement has been received
                    data_counter[number] = data_counter.get(number, 0) + 1

                    number = str(number)
                    value = str(value)
                    timeStamp = str(timeStamp)
                    data += DELIMITER.join([number, value, timeStamp]) + "\n"
                    item_count += 1

            except json.decoder.JSONDecodeError as error:
                print(common_utils.getTimeString(), "Unable to parse JSON from data: {}.".format(str(error)))

            except Exception as error:
                print(common_utils.getTimeString(), "UnExpected error while processing data for log file.")
                print(str(error))

        if data != "":
            logfile.write(data)
            # data is not immediately written to the file. when testing you can uncomment the next line to do so
            # logfile.flush()

        PROCEM_DB_QUEUE.task_done()

    logfile.close()
    writeDataCounterToFile(day + counter_name, data_counter)
    print(common_utils.getTimeString(), "CSVLogWorker handled", item_count, "items.")


def writeDataCounterToFile(filename, data_counter):
    """Write information about the measurement counts to a file."""
    DELIMITER = '\t'

    with open(filename, "a") as open_file:
        for number in sorted(data_counter):
            count = data_counter[number]
            row = DELIMITER.join([str(number), str(count)]) + "\n"
            open_file.write(row)


def procemIOTTicketWorker(iott_c):
    """Worker for handling the received packets and sending the appropriate information to IoT-Ticket."""
    # in seconds the time interval in which the buffer size is checked
    BUFFER_SIZE_CHECK_INTERVAL = 0.9 * IOTTICKET_MINIMUM_DELAY
    TIMEOUT_INTERVAL = 30  # in seconds the timeout for getting new items from the queue
    buffer_size = IOTTICKET_WRITING_BUFFER  # at least this many items are sent to IoT-Ticket as one http packet
    # if the buffer reaches this size, a new send is started even if the maximum number of active senders are in use
    MAX_BUFFER_SIZE = 1000 * IOTTICKET_MAX_PACKET_SIZE
    last_buffer_size_check_time = time.time()  # the last time the buffer size was checked
    last_send_time = last_buffer_size_check_time  # the last time a new http sending was started
    iott_buffer = []  # the buffer for holding received items
    active_counter = common_utils.Counter()  # thread-safe counter for holding the number of active senders

    data_info = set()  # a set containing the rtl_ids of items that have been received at least once
    item = None  # the current item

    worker_running = True
    item_counter = common_utils.Counter()  # this counts the number of sent items (for testing)
    while worker_running or len(iott_buffer) > 0:
        try:
            if worker_running:
                item = PROCEM_IOTTICKET_QUEUE.get(timeout=TIMEOUT_INTERVAL)
            if item is None:
                worker_running = False
                buffer_size = max(1, len(iott_buffer))
        except queue.Empty:
            # Since no new items have arrived, change the buffer size, so that any waiting items can be send.
            buffer_size = max(1, min(buffer_size, len(iott_buffer)))
            item = None

        if item is not None:
            iott_buffer += procemUDPtoIOTTicketList(item["data"], data_info)

        # Do periodic checks on the buffer size to see if it can be lowered
        current_time = time.time()
        active_workers = active_counter.getValue()
        if current_time - last_buffer_size_check_time > BUFFER_SIZE_CHECK_INTERVAL:
            if active_workers < PROCEM_IOTTICKET_WORKERS / 2:
                buffer_size = max(buffer_size // 2, len(iott_buffer), 1)
            last_buffer_size_check_time = current_time

        if len(iott_buffer) >= buffer_size:
            if (len(iott_buffer) < MAX_BUFFER_SIZE and
               (active_workers >= PROCEM_IOTTICKET_WORKERS or current_time - last_send_time < IOTTICKET_MINIMUM_DELAY)):
                # increase the buffer size if maximum number of IoT-Ticker workers has already been started
                # or there has not been enough time after previous send and the buffer is full
                buffer_size += IOTTICKET_WRITING_BUFFER
                last_buffer_size_check_time = current_time
            else:
                # Due to the slow response time of IoTTicket, we spawn a thread that makes the sending
                last_send_time = current_time
                active_counter.increase()
                iott_worker_thread = threading.Thread(
                    target=procemIOTTicketWriterThread, daemon=DAEMON_THREAD_WORKERS,
                    kwargs={
                        'iott_buffer': iott_buffer,
                        'iott_c': iott_c,
                        'counter': active_counter,
                        'item_counter': item_counter})
                iott_worker_thread.start()
                # NOTE: iott_buffer.clear() is not correct way to clear buffer in this case,
                # the ownership is moved to the thread, and clear will clear it on thread too,
                # which is not the intended use. Thus only way is to instantiate a new buffer
                iott_buffer = []

        if item is not None:
            PROCEM_IOTTICKET_QUEUE.task_done()
            item = None

    if not DAEMON_THREAD_WORKERS:
        remaining_workers = active_counter.getValue()
        if remaining_workers > 0:
            print(common_utils.getTimeString(), threading.current_thread().name, "is waiting for",
                  active_counter.getValue(), "IoT Ticket workers.")
        while remaining_workers > 0:
            time.sleep(1)
            remaining_workers = active_counter.getValue()

    print(common_utils.getTimeString(), "IOTTicketWorker handled", item_counter.getValue(), "items.")


def procemUDPtoIOTTicketList(udpdata, data_info):
    """Transforms and returns the received udpdata to a list of json objects."""
    iott_buffer = []
    for jsonitem, cycle_number in udpdata:
        # don't send to IoT-Ticket if marked as confidential
        # or if the data has been cycled too many times already
        if jsonitem.get("secret", False) or cycle_number > IOTTICKET_MAX_DATA_CYCLES:
            continue

        iot_item = {
            "name": jsonitem["name"],
            "path": jsonitem["path"],
            "v": jsonitem["v"],
            "ts": jsonitem["ts"]
        }

        # for cycled data, attribute id is not set
        rtl_id = jsonitem.get("id", None)
        if rtl_id is not None and rtl_id not in data_info:
            # when encountering a new measurement type, include also unit and dataType
            data_info.add(rtl_id)

        # NOTE: it seems that IoT-Ticket sometimes loses the unit information, if they
        # are not included every time. That's why these are now outside the previous if statement.
        # NOTE: possible reason could be that the first item which had the unit information
        # was not actually the first such datanode send, and Iot-Ticket doesn't change the unit
        # even if a different one is provided later.
        iot_item["unit"] = jsonitem["unit"]
        iot_item["type"] = jsonitem["type"]

        iott_buffer.append((iot_item, cycle_number))
    return iott_buffer


def procemIOTTicketWriterThread(iott_buffer, iott_c, counter, item_counter):
    """This is a spawnable thread that sends one HTTP packet to IoTTicket and dies after success.
       It measures how long it takes from here until we get a response."""
    # print(common_utils.getTimeString(), threading.current_thread().name, "started.", len(iott_buffer), "items.",
    #       counter.getValue(), "workers.")

    # data buffer is a list of tuples (ticket_item, cycle_number)
    # sort the data buffer in the hopes of speeding up the transfer
    iott_buffer.sort(key=lambda x: (x[0]["path"], x[0]["name"], x[0]["ts"]))
    iott_data = [item_with_cycle_number[0] for item_with_cycle_number in iott_buffer]

    startTime = time.time()

    in_progress = True
    try_number = 1
    n_packets = math.ceil(len(iott_buffer) / IOTTICKET_MAX_PACKET_SIZE)
    extra_wait = 0  # this can be set to higher value if extra wait time between retries is needed
    confirmed_written = 0
    considered_packets = set(range(n_packets))
    while in_progress and try_number <= IOTTICKET_MAXIMUM_RETRIES:
        # if this is not the first try, sleep for a few seconds before trying again
        if try_number > 1:
            time.sleep(try_number * IOTTICKET_MINIMUM_DELAY * (random.uniform(1.0, 2.0) + extra_wait))
            print(common_utils.getTimeString(), " IoTT Try ", try_number, ": ", confirmed_written,
                  "/", len(iott_buffer), " ", considered_packets, sep="")

        bad_devices = set()
        try:
            # Use the SimpleIoTTicketClient class to avoid having to use datanodesvalue class
            responces = iott_c.writeData(PROCEM_DEVICEID, iott_data, IOTTICKET_MAX_PACKET_SIZE, considered_packets)
            # Use the received responces to determine which packets need to be resend and the total for written nodes
            if IOTTICKET_VERSION == "new":
                if len(responces) == 0:
                    print("IoT-TICKET sent no status codes => will try resending the data.")
                    bad_devices = {[item["path"] for item in iott_data]}
                    extra_wait = 2.0
                else:
                    for device_id, status_code in responces:
                        if status_code is None or status_code // 100 == 4:
                            print(f"IoT-TICKET sent status code: {status_code} for {device_id} => will try resending the data.")
                            bad_devices.add(device_id)
                            extra_wait = 2.0
                        else:
                            confirmed_written += len([item for item in iott_data if item["path"] == device_id])

                    if len(bad_devices) == 0:
                        extra_wait = 0
                        in_progress = False
                    else:
                        try_number += 1
            else:
                (total_written, extra_wait_check) = iotticket_utils.getResponceInfo(
                    responces=responces,
                    n_measurements=len(iott_buffer),
                    packet_size=IOTTICKET_MAX_PACKET_SIZE,
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
            # print(common_utils.getTimeString(), " ERROR: ", threading.current_thread().name, ", IoTT failed on try ",
            #       try_number, sep="")
            responces = getattr(error, "responces", [None] * n_packets)
            (total_written, extra_wait_check) = iotticket_utils.getResponceInfo(
                responces=responces,
                n_measurements=len(iott_buffer),
                packet_size=IOTTICKET_MAX_PACKET_SIZE,
                considered_packets=considered_packets)
            confirmed_written += total_written
            extra_wait = 3.0  # wait a bit of extra time before resend because there was a connection failure
            try_number += 1

    if in_progress:
        # cycle the the items still in considered_packets back to the item queue
        if IOTTICKET_VERSION == "new":
            cycle_count = cycleBadPacketsNew(iott_buffer, bad_devices)
        else:
            cycle_count = cycleBadPackets(iott_buffer, considered_packets)
        # for printing the correct number of tries
        try_number -= 1
    else:
        cycle_count = 0

    time_diff = round(time.time() - startTime, 1)
    print(common_utils.getTimeString(), " IoTT: ", confirmed_written, "/", len(iott_buffer), ", ", sep="", end="")
    if try_number == 1:
        print(try_number, "try", end="")
    else:
        print(try_number, "tries", end="")
    print(", (", counter.getValue(), " thrds), ", "Time: ", time_diff, " seconds", sep="")
    if cycle_count > 0:
        print(common_utils.getTimeString(), " ", cycle_count, "items cycled back to the item queue")

    counter.decrease()
    item_counter.increase(confirmed_written)

    # NOTE: this is not required, but hopefully speeds up the garbage collection
    iott_buffer.clear()


def cycleBadPackets(item_buffer, bad_packets):
    """Cycles the items that weren't send to the IoT-Ticket back to the item queue."""
    cycle_count = 0

    for index in bad_packets:
        bad_list = item_buffer[index * IOTTICKET_MAX_PACKET_SIZE:(index + 1) * IOTTICKET_MAX_PACKET_SIZE]
        cycle_count += putItemsToQueue(
            item_list=bad_list,
            buffer_size=IOTTICKET_CYCLE_BUFFER_SIZE,
            item_queue=PROCEM_IOTTICKET_QUEUE,
            cycle_limit=IOTTICKET_MAX_DATA_CYCLES)

    return cycle_count


def cycleBadPacketsNew(item_buffer, bad_devices):
    """Cycles the items that weren't send to the IoT-Ticket back to the item queue."""
    cycle_count = 0

    for item, cycle_count in item_buffer:
        if cycle_count > IOTTICKET_MAX_DATA_CYCLES:
            continue
        if item["path"] in bad_devices:
            PROCEM_IOTTICKET_QUEUE.put(getProcemQueueItem([(item, cycle_count + 1)]))
            cycle_count += 1

    return cycle_count


def putItemsToQueue(item_list, buffer_size, item_queue, cycle_limit):
    """Puts items to a queue if they have not yet been cycled too many times.
         item_list = a list of the item tuples (item, cycle_number)
         buffer_size = how many items are grouped together when they are put into the queue
         item_queue = the queue in which the items are added
         cycle_attr = the item attribute that tells how many times the item has been cycled
         cycle_limit = the maximum number of times an item can be cycled
       Returns the number of items that were added to the queue.
    """
    item_count = 0
    item_buffer = []
    for item, cycle_number in item_list:
        if cycle_number > cycle_limit:
            # item has been cycled too many times already, just give up on it
            # print(common_utils.getTimeString(), "Giving up on:", item)
            continue
        item_buffer.append((item, cycle_number + 1))
        item_count += 1

        if len(item_buffer) >= buffer_size:
            item_queue.put(getProcemQueueItem(item_buffer))
            item_buffer = []

    if len(item_buffer) > 0:
        item_queue.put(getProcemQueueItem(item_buffer))

    return item_count


def procemIOTTicketTestWorker():
    """Printing version of the IoT-Ticket worker. For testing."""
    while True:
        item = PROCEM_IOTTICKET_QUEUE.get()
        if item is None:
            # what would chutulululululu do?
            break
        print("IOTTicket entry " + str(item["data"]))
        PROCEM_IOTTICKET_QUEUE.task_done()


def handleCommand(text):
    """Simple parser for changing some parameters at runtime.
       The commands can be used for example to turn the IoT-Ticket sending on or off."""
    try:
        global DB_STORAGE_CHECK
        global IOTTICKET_SEND_CHECK
        global BATTERY_DEMO_CHECK
        global IDS_FOR_BATTERY

        text = text.lower()
        if text == "" or text == "exit":
            return True
        elif text == "list":
            # Print the current value of the settings that can be changed using the parser.
            print("db-store:", DB_STORAGE_CHECK)
            print("iot-ticket:", IOTTICKET_SEND_CHECK)
            print("battery-demo:", BATTERY_DEMO_CHECK, IDS_FOR_BATTERY)
            return False

        words = text.strip().split(" ")
        if len(words) >= 2:
            command = words[0]
            parameter = words[1]
            extras = words[2:]
            if command == "db-store":
                # Turn the local storage of the data on or off.
                if parameter == "on":
                    DB_STORAGE_CHECK = True
                elif parameter == "off":
                    DB_STORAGE_CHECK = False
                else:
                    print("Unknown command! Usage: db-store on|off")
            elif command == "iot-ticket":
                # Turn the IoT-Ticket data sending on or off.
                if parameter == "on":
                    IOTTICKET_SEND_CHECK = True
                elif parameter == "off":
                    IOTTICKET_SEND_CHECK = False
                else:
                    print("Unknown command! Usage: iot-ticket on|off")
            elif command == "battery-demo":
                # Choose the data and turn the data sending to the battery demo program on or off.
                if parameter == "on":
                    BATTERY_DEMO_CHECK = True
                elif parameter == "off":
                    BATTERY_DEMO_CHECK = False
                elif parameter == "add":
                    for id in extras:
                        IDS_FOR_BATTERY.add(int(id))
                elif parameter == "remove":
                    for id in extras:
                        IDS_FOR_BATTERY.remove(int(id))
                else:
                    print("Unknown command! Usage battery-demo on|off|add [ids]|remove [ids]")
            else:
                print("Unknown command!")
        else:
            print("Unknown command!")

    except:
        print("Error in command")
        return False


if __name__ == "__main__":
    if len(sys.argv) == 2:
        PROCEM_CONF_FILE = sys.argv[1]
    elif len(sys.argv) != 1:
        print("Start this program with 'python3", sys.argv[0], "config_file.json' command")
        print("or use 'python3 ", sys.argv[0], "' to use the default configuration filename: ",
              PROCEM_CONF_FILE, sep="")
        quit()

    print(common_utils.getTimeString(), "Reading configuration parameters.")
    jd = common_utils.readConfig(PROCEM_CONF_FILE)
    PROCEM_DEVICEID = jd["deviceid"]
    IOTTICKET_DEVICES = jd.get("devices", IOTTICKET_DEVICES)
    PROCEM_USERNAME = jd["username"]
    PROCEM_PASSWORD = jd["password"]
    PROCEM_BASEURL = jd["baseurl"]
    IOTTICKET_VERSION = jd.get("iotticket-version", IOTTICKET_VERSION)
    if IOTTICKET_VERSION != "new":
        IOTTICKET_VERSION = "old"  # only "new" and "old" are allowed values
    DB_STORAGE_CHECK = jd.get("db_storage_on", DB_STORAGE_CHECK)
    IOTTICKET_SEND_CHECK = jd.get("iotticket_send_on", IOTTICKET_SEND_CHECK)
    BATTERY_DEMO_CHECK = jd.get("battery_demo_on", BATTERY_DEMO_CHECK)
    IOTTICKET_WRITING_BUFFER = jd.get("iotticket-buffer-size", IOTTICKET_WRITING_BUFFER)
    IOTTICKET_MAX_PACKET_SIZE = jd.get("iotticket-max-packet-size", IOTTICKET_MAX_PACKET_SIZE)
    IOTTICKET_MINIMUM_DELAY = jd.get("iotticket-minimum-delay-s", IOTTICKET_MINIMUM_DELAY)
    IOTTICKET_MAXIMUM_RETRIES = jd.get("iotticket-maximum-retries", IOTTICKET_MAXIMUM_RETRIES)
    IOTTICKET_MAX_DATA_CYCLES = jd.get("iotticket-max-data-cycles", IOTTICKET_MAX_DATA_CYCLES)
    PROCEM_IOTTICKET_WORKERS = jd.get("procem-iotticket-workers", PROCEM_IOTTICKET_WORKERS)
    iotticketQueueSize = jd.get("iotticket-queue-size", IOTTICKET_QUEUE_SIZE)
    dbQueueSize = jd.get("db-queue-size", DB_QUEUE_SIZE)
    dbType = jd.get("db_type", "csv")
    PRESENT_VALUE_COUNT = jd.get("present_value_count", PRESENT_VALUE_COUNT)

    ids_for_battery_list = jd.get("ids_for_battery", [])
    for id in ids_for_battery_list:
        IDS_FOR_BATTERY.add(id)

    # create queues for worker threads
    # maxsize could be infinite, but it is good idea to set maximum size to prevent exhaustion in a case of malfunction.
    PROCEM_DB_QUEUE = queue.Queue(maxsize=dbQueueSize)
    PROCEM_IOTTICKET_QUEUE = queue.Queue(maxsize=iotticketQueueSize)

    PROCEM_MAIN_QUEUE = queue.Queue(maxsize=max(dbQueueSize, iotticketQueueSize))
    PROCEM_VALUE_QUERY_QUEUE = queue.Queue(maxsize=max(dbQueueSize, iotticketQueueSize))
    PROCEM_BATTERY_QUEUE = queue.Queue(maxsize=max(dbQueueSize, iotticketQueueSize))

    # create the dictionary that holds the present values of the measurements
    PRESENT_VALUES = datastorage.DataStorage(PRESENT_VALUE_COUNT)

    # sanity check:
    # TODO: add some checks for the other parameters as well (especially the numerical parameters)
    if PROCEM_DEVICEID == "" or PROCEM_USERNAME == "" or PROCEM_PASSWORD == "" or PROCEM_BASEURL == "":
        print("ERROR: Configuration file not complete. Terminating")
        quit()

    threads = []

    # for data validation and forwarding as well as for keeping the presentValue table updated.
    validation_thread = threading.Thread(
        target=validationWorker, name="ProcemValidationWorker", daemon=DAEMON_THREAD_WORKERS)
    validation_thread.start()
    threads.append(validation_thread)

    # for handling queries related to the present values of measurements
    value_query_thread = threading.Thread(
        target=presentValueQueryWorker, name="ProcemValueQueryWorker", daemon=DAEMON_THREAD_WORKERS)
    value_query_thread.start()
    threads.append(value_query_thread)

    # for handling the data sending to the battery demo program
    battery_thread = threading.Thread(
        target=batteryWorker, name="ProcemBatteryWorker", daemon=DAEMON_THREAD_WORKERS)
    battery_thread.start()
    threads.append(battery_thread)

    # for handling the local storage of the data
    dbWorkers = {"json": procemDBWorker, "csv": procemCSVLogWorker}
    tdb = threading.Thread(target=dbWorkers[dbType], name="ProcemLogWorker", daemon=DAEMON_THREAD_WORKERS)
    tdb.start()
    threads.append(tdb)

    # for handling the data sending to the IoT-Ticket
    iott_c = iotticket_utils.SimpleIoTTicketClient(PROCEM_BASEURL, PROCEM_USERNAME, PROCEM_PASSWORD, IOTTICKET_VERSION, IOTTICKET_DEVICES)
    tiot = threading.Thread(
        target=procemIOTTicketWorker, name="ProcemIOTTicketWorker",
        kwargs={'iott_c': iott_c}, daemon=DAEMON_THREAD_WORKERS)
    tiot.start()
    threads.append(tiot)

    # for receiving data from the adapter programs
    # srv = common_utils.startUDPserver(handler=ReceivedUDPHandler, ip=PROCEM_SERVER_IP, port=PROCEM_SERVER_PORT)
    print(common_utils.getTimeString(), "Starting the UDP server for Procem RTL")
    connection_try_interval = 0.0
    connection_try_increase = 5.0
    srv = None
    while srv is None:
        try:
            srv = common_utils.startUDPserver(handler=ReceivedUDPHandler, ip=PROCEM_SERVER_IP, port=PROCEM_SERVER_PORT)
        except:
            connection_try_interval += connection_try_increase
            print(common_utils.getTimeString(), "Could not start UDP server. Trying again in",
                  connection_try_interval, "seconds")
            time.sleep(connection_try_interval)

    while True:
        txt = input("Give command or press enter key to end:\n\r")
        if handleCommand(txt):
            break

    common_utils.stopUDPserver(srv)
    PROCEM_MAIN_QUEUE.put(None)
    if not DAEMON_THREAD_WORKERS:
        for worker_thread in threads:
            worker_thread.join()
            print(common_utils.getTimeString(), worker_thread.name, "closed.")
