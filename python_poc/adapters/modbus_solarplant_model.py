# -*- coding: utf-8 -*-
"""This module contains the data model for the solar plant measurements."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

try:
    import adapters.common_utils as common_utils
    import adapters.modbus_generic_model as modbus_generic_model
except:
    # used when running the module directly
    import common_utils
    import modbus_generic_model


class SolarPlantMeasurementType(modbus_generic_model.ModbusMeasurementType):
    """This class holds the information about the different measurement types for the solar plant.
         decimals   = how many digits after decimal point does the measurement have
    """

    def __init__(
            self, name, path, rtl_id, read_once, ticket, reg_type, unit, data_type,
            count, signed, word_order, byte_order, nan, decimals):
        super().__init__(
            name, path, rtl_id, read_once, ticket, reg_type, unit, data_type,
            count, signed, word_order, byte_order, nan)
        self.__decimals = decimals

    @property
    def decimals(self):
        return self.__decimals

    def calculateValue(self, register_values):
        """Calculates and returns the actual measurement value from the registers."""
        value = super().calculateValue(register_values)
        if value is not None:
            # Convert the floating point measurements to decimal values
            if self.data_type == "double" or self.data_type == "float":
                value = round(value / 10**self.decimals, self.decimals)

        return value


class SolarPlantFieldStorage(modbus_generic_model.FieldStorage):
    def __init__(self, field_data):
        super().__init__(field_data)
        self.decimal_field = field_data["decimals"]
        self.ticket_field = field_data["iot_ticket"]


def createSolarPlantMeasurement(field_info, data):
    return SolarPlantMeasurementType(
        name=data[field_info.name_field],
        path=data[field_info.path_field],
        rtl_id=int(data[field_info.rtlid_field]),
        read_once=data[field_info.read_once_field] != "",
        ticket=data[field_info.ticket_field] != "",
        reg_type=data[field_info.type_field],
        unit=data[field_info.unit_field],
        data_type=data[field_info.datatype_field],
        count=int(data[field_info.count_field]),
        signed=(data[field_info.sign_field] == "y"),
        word_order=data[field_info.wordorder_field],
        byte_order=data[field_info.byteorder_field],
        nan=int(data[field_info.nan_field], 16),  # the NaN values should be given in hexadecimal format
        decimals=int(data[field_info.decimal_field]))


def getFieldStorage():
    return SolarPlantFieldStorage


def getCreateFunction():
    return createSolarPlantMeasurement
