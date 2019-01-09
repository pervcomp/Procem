# -*- coding: utf-8 -*-
"""This module contains the data model for the MXElectrix measurement device."""

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


class MXElectrixMeasurementType:
    """This class holds the information about the different measurement types.
         name   = the measurement name in IoT-Ticket
         path   = the measurement path in IoT-Ticket
         rtl_id = the measurement id used for local storage
         ticket = True, if measurement will be sent to IoT-Ticket. False, otherwise.
    """

    def __init__(self, name, path, rtl_id, ticket):
        self.__name = name
        self.__path = path
        self.__rtl_id = rtl_id
        self.__ticket = ticket

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


class MXElectrixDataModel:
    """This class is used as a data model for the Laatuvahti measurement device."""

    def __init__(self):
        self.__devices = {}

    def addMeasurementType(self, device, measurementName, measurementType):
        if device not in self.__devices:
            self.__devices[device] = {}
        self.__devices[device][measurementName] = copy.deepcopy(measurementType)

    def getMeasurementInfo(self, device, measurementName):
        if device not in self.__devices or measurementName not in self.__devices[device]:
            return None
        else:
            return self.__devices[device][measurementName]


def load_model(csvFilename, configFilename):
    """Reads Laatuvahti measurement type information from csv file using a json configuration file.
       Returns an object of MXElectrixDataModel class that can be used to access the measurement information.
       By default measurements which have the name 'vara' in the csv file in the the column 'laatuvahti_name'
       are ignored, this can be changed by editing the configuration file (the json file).
    """

    try:
        # Load the configuration scheme from a json file.
        config = common_utils.readConfig(configFilename)

        deviceField = config["csv_header"]["device"]
        measurementNameField = config["csv_header"]["measurementName"]
        nameField = config["csv_header"]["name"]
        pathField = config["csv_header"]["path"]
        idField = config["csv_header"]["rtl_id"]
        ticketField = config["csv_header"]["ticket"]

        # Measurements that have the same name as ignoreMark are ignored.
        ignoreMark = config["ignore_mark"]
        # Measurements are sent to IoTTicket if the ticketField matches ticketMark.
        ticketMark = config["ticket_mark"]

        dataModel = MXElectrixDataModel()

        # Load the measurement information from a csv file.
        with open(csvFilename, mode="r") as csvFile:
            reader = csv.DictReader(csvFile, delimiter=";")

            for row in reader:
                device = str(row[deviceField])
                measurementName = row[measurementNameField]
                if measurementName == ignoreMark:
                    continue

                measurementType = MXElectrixMeasurementType(
                    name=row[nameField],
                    path=row[pathField],
                    rtl_id=int(row[idField]),
                    ticket=(row[ticketField] == ticketMark))
                dataModel.addMeasurementType(device, measurementName, measurementType)

    except Exception as error:
        # Something went wrong. Make it the problem of the caller.
        raise error

    return dataModel
