# -*- coding: utf-8 -*-
"""This module contains the data model for the ISS measurements."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import math
import struct

try:
    import adapters.common_utils as common_utils
    import adapters.modbus_generic_model as modbus_generic_model
except:
    # used when running the module directly
    import common_utils
    import modbus_generic_model


class ISSMeasurementType(modbus_generic_model.ModbusMeasurementType):
    """This class holds the information about the different measurement types for the solar plant.
         modbus_datatype = the modbus datatype that is in use (e.g. Float32 or UInt32)
    """

    def __init__(
            self, name, path, rtl_id, read_once, ticket, reg_type, unit, data_type,
            count, signed, word_order, byte_order, nan, modbus_datatype, decimals):
        super().__init__(
            name, path, rtl_id, read_once, ticket, reg_type, unit, data_type,
            count, signed, word_order, byte_order, nan)
        self.__modbus_datatype = modbus_datatype
        self.__decimals = decimals

    @property
    def decimals(self):
        return self.__decimals

    @property
    def modbus_datatype(self):
        return self.__modbus_datatype

    def calculateValue(self, register_values):
        """Calculates and returns the actual measurement value from the registers."""
        if self.modbus_datatype == "Mod10":
            if self.word_order == "little":
                register_values = reversed(register_values)

            # assume big endian byte order and unsigned values when the datatype is Mod10
            # also no checks for possible NaN values are done here
            value = 0
            for register in register_values:
                value *= 10**4
                value += register % 10**4
            return value

        elif self.modbus_datatype == "Float32":
            if self.word_order == "little":
                register_values = reversed(register_values)
            if self.byte_order == "little":
                endian = "<"
            else:
                endian = ">"

            # construct the IEEE-standard floating point value from the integer
            value = struct.unpack("f", struct.pack(endian + "2H", *register_values))[0]

            # if the value is NaN, set it to 0
            if math.isnan(value):
                value = 0.0
            return value

        value = super().calculateValue(register_values)
        if value is not None:
            # Convert the floating point measurements to decimal values
            if self.modbus_datatype == "Decimal":
                value = round(value / 10**self.decimals, self.decimals)
            else:
                # TODO: handle other non integer data types
                pass

        return value


class ISSFieldStorage(modbus_generic_model.FieldStorage):
    def __init__(self, field_data):
        super().__init__(field_data)
        self.confidential_field = field_data["confidential"]
        self.ticket_field = field_data["iot_ticket"]
        self.modbus_datatype_field = field_data["modbus_datatype"]
        self.decimal_field = field_data["decimals"]


def createISSMeasurement(field_info, data):
    decimals = data[field_info.decimal_field]
    if decimals == "":
        decimals = 0
    return ISSMeasurementType(
        name=data[field_info.name_field],
        path=data[field_info.path_field],
        rtl_id=int(data[field_info.rtlid_field]),
        read_once=data[field_info.read_once_field] != "",
        ticket=data[field_info.confidential_field] == "" and data[field_info.ticket_field] != "",
        reg_type=data[field_info.type_field],
        unit=data[field_info.unit_field],
        data_type=data[field_info.datatype_field],
        count=int(data[field_info.count_field]),
        signed=(data[field_info.sign_field] == "y"),
        word_order=data[field_info.wordorder_field],
        byte_order=data[field_info.byteorder_field],
        nan=int(data[field_info.nan_field], 16),  # the NaN values should be given in hexadecimal format
        modbus_datatype=data[field_info.modbus_datatype_field],
        decimals=int(decimals))


def getFieldStorage():
    return ISSFieldStorage


def getCreateFunction():
    return createISSMeasurement
