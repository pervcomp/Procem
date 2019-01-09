# -*- coding: utf-8 -*-
"""This module contains an adapter for reading Modbus data and sending it to Procem RTL worker."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

from pymodbus.client.sync import ModbusTcpClient as ModbusClient
import datetime
import queue
import socket
import threading
import time
import sys

try:
    import adapters.common_utils as common_utils
    import adapters.modbus_generic_model as modbus_generic_model
    import adapters.modbus_utils as modbus_utils
except:
    # used when running the module directly
    import common_utils
    import modbus_generic_model
    import modbus_utils

PROCEM_SERVER_IP = common_utils.PROCEM_SERVER_IP
PROCEM_SERVER_PORT = common_utils.PROCEM_SERVER_PORT

# maximum size for UDP payload. Current value taken from mxelectrix_adapter
UDP_MAX_SIZE = common_utils.UDP_MAX_SIZE

# To reduce UDP traffic buffer the data sending to procem_rtl using this global queue
data_queue = queue.Queue()

# The default names of the configuration files from where the data model information is read
CONFIG_SCHEME_FILE_NAME = "modbus_solarplant_config.json"
MEASUREMENT_ID_FILE_NAME = "Solar_Plant_measurement_IDs_v3.csv"

# The default name of the model file
MODEL_NAME = "modbus_generic_model"

# The supported register types and their reading functions
SUPPORTED_REGISTER_TYPES = {
    "input": "read_input_registers",
    "holding": "read_holding_registers"
}


def readRegisterGroups(client, device, register_groups, measurement_queue):
    """Reads several groups of Modbus registers and sends the data to Procem RTL."""
    delay = device.delay / 1000  # time interval between reading different registers in seconds
    unitid = device.unit_id

    for register_group in register_groups:
        start_register = register_group.start_register
        register_count = register_group.register_count
        register_type = register_group.type
        if register_type not in SUPPORTED_REGISTER_TYPES:
            print(common_utils.getTimeString(), "ERROR: Register type", register_type, "not supported")
            continue

        try:
            # time1 = time.time()
            current_start_register = start_register
            current_register_count_max = register_count
            current_register_count = current_register_count_max
            received_registers = []
            timestamps = []
            while len(received_registers) < register_count and current_register_count > 0:
                # time1 = time.time()
                resp = getattr(client, SUPPORTED_REGISTER_TYPES[register_type])(
                    current_start_register, current_register_count, unit=unitid)
                tm = int(round(time.time() * 1000))
                # time2 = time.time()

                if getattr(resp, "registers", None) is None:
                    # print("failed: ", unitid, ": ", current_start_register, "-",
                    #       current_start_register + current_register_count - 1,
                    #       " (", current_register_count, "), read time: ", time2 - time1, sep="")
                    current_register_count_max //= 2
                else:
                    # print("success: ", unitid, ": ", current_start_register, "-",
                    #       current_start_register + current_register_count - 1,
                    #       " (", current_register_count, "), read time: ", time2 - time1, sep="")
                    received_registers += resp.registers
                    timestamps += [tm] * len(resp.registers)
                    current_start_register += len(resp.registers)

                if len(received_registers) < register_count:
                    old_end_register = current_start_register - 1
                    (current_start_register, current_end_register) = register_group.getPart(
                        current_start_register, current_register_count_max)

                    if current_start_register is None or current_end_register is None:
                        current_register_count = 0
                    else:
                        skipped_registers = current_start_register - old_end_register - 1
                        if skipped_registers > 0:
                            received_registers += [0] * skipped_registers
                            timestamps += [tm] * skipped_registers
                        current_register_count = current_end_register - current_start_register + 1

                if len(received_registers) < register_count and current_register_count > 0:
                    time.sleep(delay / 10)

            if len(received_registers) >= register_count:
                # time2 = time.time()
                # print("success: ", unitid, ": registers: ", start_register, "-",
                #       start_register + register_count - 1, ", read time: ", time2 - time1, sep="")
                measurement_queue.put({
                    "register_group": register_group,
                    "response_data": received_registers,
                    "timestamps": timestamps})
            else:
                print(common_utils.getTimeString(), " ERROR: (", device.ip, ", ", unitid,
                      "): Failure to read registers: ", start_register, "-", start_register + register_count - 1,
                      " (", resp, ")", sep="")

        except Exception as error:
            print(common_utils.getTimeString(), " ERROR: could not read registers ", start_register,
                  "-", start_register + register_count - 1, " from (", device.ip, ", ", unitid, ")", sep="")
            print(error)

        # Sleep for a little bit before reading the next register group
        time.sleep(delay)


def sendMeasurementsToProcem(device, measurement_queue):
    """Sends a collection of measurements to Procem RTL."""
    while True:
        measurement = measurement_queue.get()
        if measurement is None:
            break

        register_group = measurement["register_group"]
        response_data = measurement["response_data"]
        timestamps = measurement["timestamps"]
        for register_id, count in register_group.registers:
            index = register_id - register_group.start_register
            register_type = device.registers[register_id]
            register_values = response_data[index:index + count]
            timestamp = timestamps[index + count - 1]

            # parse the data and create a Procem packet and put it in the data queue
            new_pkt = modbus_utils.getProcemRTLpkt(register_values, register_type, timestamp)
            data_queue.put(new_pkt)


def ModBusWorker(device):
    """Reads registers periodically from a Modbus device and sends the data to Procem RTL."""
    ip = device.ip
    port = device.port
    source_ip = device.source_ip
    source_port = device.source_port
    interval = device.interval / 1000  # time interval between reading the same register in seconds
    start_time = time.time()

    # start the measurement handling thread
    measurement_queue = queue.Queue()
    threading.Thread(
        target=sendMeasurementsToProcem,
        kwargs={"device": device, "measurement_queue": measurement_queue},
        daemon=True).start()

    kwargs = {"host": ip}
    if port is not None:
        kwargs["port"] = port
    if source_ip is not None:
        kwargs["source_address"] = (source_ip, source_port)

    kwargs["timeout"] = 300

    client = ModbusClient(**kwargs)
    client.connect()
    print(common_utils.getTimeString(), "INFO: Connected ModBus server at address " + ip)

    try:
        # First handle the read once registers
        readRegisterGroups(client, device, device.read_once_groups, measurement_queue)

        loop_count = 0
        day = datetime.date.today().day
        start_time = time.time()
        while True:
            current_day = datetime.date.today().day
            # Handle the read once register again if it is a new day
            if current_day != day:
                print(common_utils.getTimeString(), loop_count, "packages sent from", ip, "on day", day)
                readRegisterGroups(client, device, device.read_once_groups, measurement_queue)
                day = current_day
                start_time += loop_count * interval
                loop_count = 0

            current_time = time.time()
            sleep_time = loop_count * interval - (current_time - start_time)
            if sleep_time > 0:
                time.sleep(sleep_time)

            readRegisterGroups(client, device, device.groups, measurement_queue)

            loop_count += 1
            # Print the send information once in an hour.
            if loop_count % 3600 == 0:
                print(common_utils.getTimeString(), loop_count, "packages sent from", ip, "on day", day)

    except OSError as err:
        print(common_utils.getTimeString(), "ERROR: unexpected behavior in thread of IP", ip, "error was", err)
    finally:
        # sleep for safety?
        client.close()
        print(common_utils.getTimeString(), "INFO: Closing the connection to ModBus server")


def startModBusAdapter(data_model):
    """Starts separate worker thread for each Modbus device in the data model."""
    devices = data_model.devices
    for device_id, device in devices.items():
        print(common_utils.getTimeString(), "Starting thread for device", device_id)
        threading.Thread(target=ModBusWorker, kwargs={"device": device}, daemon=True).start()


if __name__ == "__main__":
    if len(sys.argv) == 3:
        CONFIG_SCHEME_FILE_NAME = sys.argv[1]
        MEASUREMENT_ID_FILE_NAME = sys.argv[2]
    elif len(sys.argv) != 1:
        print("Start this adapter with 'python3", sys.argv[0], "config_scheme.json measurement_ids.csv' command")
        print("or use 'python3 ", sys.argv[0], "' to use the default configuration.", sep="")
        quit()

    print(common_utils.getTimeString(), "Reading modbus configurations from",
          CONFIG_SCHEME_FILE_NAME, "and", MEASUREMENT_ID_FILE_NAME)
    # Read the model name from the configuration file
    config = common_utils.readConfig(CONFIG_SCHEME_FILE_NAME)
    MODEL_NAME = config.get("model_name", MODEL_NAME)

    # import the correct model code and load the model information from the configuration files
    model = __import__(MODEL_NAME)
    field_info_class = getattr(model, "getFieldStorage")
    create_measurement_function = getattr(model, "getCreateFunction")

    data_model = modbus_generic_model.loadModel(
        config_filename=CONFIG_SCHEME_FILE_NAME,
        csv_filename=MEASUREMENT_ID_FILE_NAME,
        field_model=field_info_class(),
        create_measurement=create_measurement_function())

    # start the Procem send worker that takes the values from data_queue and sends them to procem_rtl
    threading.Thread(target=common_utils.procemSendWorker, kwargs={"data_queue": data_queue}).start()

    startModBusAdapter(data_model)

    while True:
        txt = input("Press enter key to end:\n\r")
        if not txt:
            data_queue.put(None)
            break
