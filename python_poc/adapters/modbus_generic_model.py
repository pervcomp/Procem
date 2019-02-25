# -*- coding: utf-8 -*-
"""This module contains the generic data model for the modbus device measurements."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import copy
import csv
import json

try:
    import adapters.common_utils as common_utils
except:
    # used when running the module directly
    import common_utils


class ModbusMeasurementType:
    """This class holds the information about the different measurement types for a modbus device.
         name       = the measurement name in IoT-Ticket
         path       = the measurement path in IoT-Ticket
         rtl_id     = the measurement id used for local storage
         read_once  = True, if the measurement will be read only once at the beginning
         ticket     = True, if the measurement will be sent to IoT-Ticket. False, otherwise.
         type       = the type of the register
         unit       = the measurement unit
         data_type  = the data type of the measurement
         count      = the number of registers (16 bits) to read for the measurement
         signed     = True, if the measurement value is signed. False, otherwise.
         word_order = the word (16 bits) order for the measurement
         byte_order = the byte (8 bits) order for the measurement
         nan        = the value that represents NaN for the measurement
    """

    def __init__(
            self, name, path, rtl_id, read_once, ticket, reg_type, unit, data_type, count,
            signed, word_order, byte_order, nan):
        self.__name = name
        self.__path = path
        self.__rtl_id = rtl_id
        self.__ticket = ticket
        self.__read_once = read_once
        self.__type = reg_type
        self.__unit = unit
        self.__data_type = data_type
        self.__count = count
        self.__signed = signed
        self.__word_order = word_order
        self.__byte_order = byte_order
        self.__nan = nan

    @property
    def name(self):
        return self.__name

    @property
    def path(self):
        return self.__path

    @property
    def rtl_id(self):
        return self.__rtl_id

    @property
    def ticket(self):
        return self.__ticket

    @property
    def read_once(self):
        return self.__read_once

    @property
    def type(self):
        return self.__type

    @property
    def unit(self):
        return self.__unit

    @property
    def data_type(self):
        return self.__data_type

    @property
    def count(self):
        return self.__count

    @property
    def signed(self):
        return self.__signed

    @property
    def word_order(self):
        return self.__word_order

    @property
    def byte_order(self):
        return self.__byte_order

    @property
    def nan(self):
        return self.__nan

    def calculateValue(self, register_values):
        """Calculates and returns the actual measurement value from the registers."""
        if (self.data_type != "double" and self.data_type != "float" and
                self.data_type != "int" and self.data_type != "long" and self.data_type != "integer"):
            print(common_utils.getTimeString(), "ERROR: Unsupported datatype:", self.data_type)
            return None

        # Convert the 16bits registers into array of bytes
        # NOTE: SMA devices use big endian byteorder - that is 16bit word comes in with high byte first
        # NOTE: at least in SMA devices, the word order is big endian, 1st register has the higher word
        if self.word_order == "little":
            register_values = reversed(register_values)

        byte_list = []
        for word in register_values:
            first_byte = 0xFF & (word >> 8)
            second_byte = 0xFF & word
            if self.byte_order == "little":
                byte_list.append(second_byte)
                byte_list.append(first_byte)
            else:  # big endian byte order
                byte_list.append(first_byte)
                byte_list.append(second_byte)

        # Convert array of bytes into integer (positive integer for comparison with the NaN value)
        value = int.from_bytes(byte_list, byteorder="big", signed=False)

        # SMAs have NaN values like 0x8000 or 0x80000000.. whad woud makkyver do?
        if value == self.nan:
            # print(common_utils.getTimeString(), "WARNING: Zeroing received NaN value for rtl_id:", self.rtl_id)
            return 0

        if self.signed:
            return int.from_bytes(byte_list, byteorder="big", signed=self.signed)
        else:
            return value


class ModbusMeasurementGroup:
    """This class holds information about a group of registers that can read with a single query.
         type               = the type of the registers in the group
         start_register     = the smallest register in the group
         register_count     = the number of registers needed when reading the whole group
         max_register_count = the maximum number of registers the group can contain
         registers          = a list of the register ids contained in the group
    """
    def __init__(self, type, start_register, end_register, max_register_count=120):
        self.__type = type
        self.__start_register = start_register
        self.__end_register = min(end_register, start_register + max_register_count - 1)
        self.__registers = [(start_register, self.__end_register - start_register + 1)]
        self.__max_register_count = max_register_count

    @property
    def type(self):
        return self.__type

    @property
    def start_register(self):
        return self.__start_register

    @property
    def register_count(self):
        return self.__end_register - self.__start_register + 1

    @property
    def max_register_count(self):
        return self.__max_register_count

    @property
    def registers(self):
        return self.__registers

    def addRegister(self, register_type, start_register, end_register):
        # only add the new register if is of the same type and the count doesn't become too large
        new_register_count = max(
            abs(start_register - self.__end_register),
            abs(end_register - self.__start_register))
        if (register_type != self.__type or
                new_register_count >= self.max_register_count or
                self.areaInUse(start_register, end_register)):
            return False
        else:
            self.__registers.append((start_register, end_register - start_register + 1))
            self.__start_register = min(start_register, self.__start_register)
            self.__end_register = max(end_register, self.__end_register)
            self.__registers.sort(key=lambda x: x[0])
            return True

    def areaInUse(self, start_register, end_register):
        """Checks whether the given register area [start_register, end_register] is already in use."""
        for register_id, count in self.__registers:
            if register_id + count - 1 < start_register:
                continue
            else:
                return register_id <= end_register
        return False

    def getPart(self, start_register, max_count):
        start = None
        end = None
        for register_id, count in self.__registers:
            register_end = register_id + count - 1
            if register_end < start_register:
                continue
            elif start is None:
                start = max(start_register, register_id)
                if register_end >= start_register + max_count:
                    break
                else:
                    end = register_end
            elif register_end - start + 1 <= max_count:
                end = register_end
            else:
                break

        return start, end


class ModbusDevice:
    """This class holds the information about the different devices for a modbus device.
         ip          = the ip address for the device
         port        = the port for the device (if None, the default port will be used)
         source_ip   = the source ip address (if None, the default source ip will be used)
         source_port = the source port
         unit_id     = the unit id that is used when reading the register values
         interval    = the minimum time interval before reading the same register again
         delay       = waiting time in ms between reading the different register values
         max_groups  = the maximum number of regular register groups
         max_group_size   = the maximum size for a register group
         registers        = a dict of registers that can be read
                            (key is register_id, value is of type ModbusMeasurementType)
         read_once_groups = a list of groups for read once registers
         groups           = a list of groups for regular registers
    """

    def __init__(self, ip, port, unit_id, interval, delay, max_groups, max_group_size=120,
                 source_ip=None, source_port=0):
        self.__ip = ip
        self.__port = port
        self.__unit_id = unit_id
        self.__interval = interval
        self.__delay = delay
        self.__max_groups = max_groups
        self.__max_group_size = max_group_size
        self.__source_ip = source_ip
        self.__source_port = source_port
        self.__registers = {}
        self.__read_once_groups = []
        self.__groups = []

    @property
    def ip(self):
        return self.__ip

    @property
    def port(self):
        return self.__port

    @property
    def unit_id(self):
        return self.__unit_id

    @property
    def interval(self):
        return self.__interval

    @property
    def delay(self):
        return self.__delay

    @property
    def max_groups(self):
        return self.__max_groups

    @property
    def max_group_size(self):
        return self.__max_group_size

    @property
    def source_ip(self):
        return self.__source_ip

    @property
    def source_port(self):
        return self.__source_port

    @property
    def registers(self):
        return self.__registers

    @property
    def read_once_groups(self):
        return self.__read_once_groups

    @property
    def groups(self):
        return self.__groups

    def addRegister(self, register_id, register_type):
        self.__registers[register_id] = copy.deepcopy(register_type)

        if register_type.read_once:
            self.__addRegisterToGroup(register_id, register_type, self.__read_once_groups, False)
        else:
            self.__addRegisterToGroup(register_id, register_type, self.__groups, True)

    def __addRegisterToGroup(self, register_id, register_type, group_list, group_limit=True):
        end_register = register_id + register_type.count - 1
        new_register_type = register_type.type
        group_index = 0
        register_in_group = False
        while not register_in_group and group_index < len(group_list):
            register_in_group = group_list[group_index].addRegister(
                new_register_type, register_id, end_register)
            group_index += 1

        if not register_in_group:
            if len(group_list) < self.__max_groups or not group_limit:
                group_list.append(
                    ModbusMeasurementGroup(new_register_type, register_id, end_register, self.max_group_size))
            else:
                print(common_utils.getTimeString(), "Register", register_id, "not added to", self.ip)


class ModbusDataModel:
    """This class is used as a data model for modbus devices.
         devices = a dict of added devices (key is device_id, value is of type ModbusDevice or its subclass)
    """

    def __init__(self):
        self.__devices = {}

    @property
    def devices(self):
        return copy.deepcopy(self.__devices)

    def addDevice(self, device_id, device):
        self.__devices[device_id] = copy.deepcopy(device)

    def addRegister(self, device_id, register_id, register_type):
        if device_id not in self.__devices:
            print(common_utils.getTimeString(), "ERROR: No device", device_id, "found. Register",
                  register_id, "not added.")
        else:
            self.__devices[device_id].addRegister(register_id, register_type)


class FieldStorage:
    def __init__(self, field_data):
        self.device_field = field_data["device_id"]
        self.rtlid_field = field_data["rtl_id"]
        self.register_field = field_data["register_id"]
        self.type_field = field_data["register_type"]
        self.name_field = field_data["name"]
        self.path_field = field_data["path"]
        self.unit_field = field_data["unit"]
        self.datatype_field = field_data["datatype"]
        self.count_field = field_data["count"]
        self.sign_field = field_data["signed"]
        self.wordorder_field = field_data["wordorder"]
        self.byteorder_field = field_data["byteorder"]
        self.nan_field = field_data["NaN"]
        self.read_once_field = field_data["read_once"]
        self.secondary_field = field_data["secondary"]


def createMeasurementType(field_info, data):
    return ModbusMeasurementType(
        name=data[field_info.name_field],
        path=data[field_info.path_field],
        rtl_id=int(data[field_info.rtlid_field]),
        read_once=data[field_info.read_once_field] != "",
        ticket=True,  # all measurements can be sent to the IoT-Ticket
        reg_type=data[field_info.type_field],
        unit=data[field_info.unit_field],
        data_type=data[field_info.datatype_field],
        count=int(data[field_info.count_field]),
        signed=(data[field_info.sign_field] == "y"),
        word_order=data[field_info.wordorder_field],
        byte_order=data[field_info.byteorder_field],
        nan=int(data[field_info.nan_field], 16))  # the NaN values should be given in hexadecimal format


def loadConfigurations(config_filename, field_model):
    """Reads modbus device configurations and csv layout information from a json configuration
       file and returns the corresponding model and field information."""
    # Load the configuration scheme from a json file.
    config = common_utils.readConfig(config_filename)

    devices = config["devices"]
    data_model = ModbusDataModel()
    for device_id, device_info in devices.items():
        device = ModbusDevice(
            ip=device_info["ip"],
            port=device_info.get("port", None),
            unit_id=device_info["unit_id"],
            interval=device_info["interval_ms"],
            delay=device_info["delay_ms"],
            max_groups=device_info["max_groups"],
            source_ip=device_info.get("source_ip", None),
            source_port=device_info.get("source_port", 0),
            max_group_size=device_info.get("max_group_size", 120))
        data_model.addDevice(int(device_id), device)

    csv_header = config["csv_header"]
    field_info = field_model(csv_header)

    return data_model, field_info


def loadModel(config_filename, csv_filename, field_model, create_measurement):
    """Reads modbus device measurement type information from csv file using a json configuration file.
       Returns the data model that can be used to access the measurement information.
    """
    try:
        # Load the device configurations and field information from a json file.
        (data_model, field_info) = loadConfigurations(config_filename, field_model)

        secondary_registers = []

        # Load the measurement information from a csv file.
        with open(csv_filename, mode="r") as csv_file:
            reader = csv.DictReader(csv_file, delimiter=";")

            for row in reader:
                device_id = int(row[field_info.device_field])
                register_id = int(row[field_info.register_field])

                register_type = create_measurement(field_info, row)

                if row[field_info.secondary_field] != "":
                    secondary_registers.append((device_id, register_id, register_type))
                else:
                    data_model.addRegister(device_id, register_id, register_type)

        for register in secondary_registers:
            data_model.addRegister(register[0], register[1], register[2])

        # print("Created register groups")
        # for device_id, device in data_model.devices.items():
        #     for index, group in enumerate(device.read_once_groups + device.groups):
        #         print(device_id, index, group.type, group.start_register, group.register_count)
        #         for register in group.registers:
        #             print(" ", register[0], end="")
        #         print()

    except Exception as error:
        # Something went wrong. Make it the problem of the caller.
        raise error

    return data_model


def getFieldStorage():
    return FieldStorage


def getCreateFunction():
    return createMeasurementType
