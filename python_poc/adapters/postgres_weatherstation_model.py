# -*- coding: utf-8 -*-
"""This module contains the data model for the weather station measurements."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import copy
import csv
import datetime
import json
import threading
import time

try:
    import adapters.common_utils as common_utils
    import adapters.postgres_utils as postgres_utils
except:
    # used when running the module directly
    import common_utils
    import postgres_utils


class WeatherStationMeasurementType:
    """This class holds the information about the different measurement types for the weather station.
        name      = the measurement name in IoT-Ticket
        path      = the measurement path in IoT-Ticket
        rtl_id    = the measurement id used for local storage
        ticket    = True, if measurement will be sent to IoT-Ticket. False, otherwise.
        unit      = the measurement unit
        data_type = the data type of the measurement
    """

    def __init__(self, name, path, rtl_id, ticket, unit, data_type):
        self.__name = name
        self.__path = path
        self.__rtl_id = rtl_id
        self.__ticket = ticket
        self.__unit = unit
        self.__data_type = data_type

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
    def unit(self):
        return self.__unit

    @property
    def data_type(self):
        return self.__data_type


class WeatherStationTable:
    """This class holds the information about the different devices for the solar plant.
        interval    = the time interval in milliseconds on which data is gathered from the table
        delay       = the time interval in milliseconds for how often the table is accessed
        time_field  = the field which has the time stamp for the table
        fields      = a dict of the measurement fields in the table
                      (key is field_name, value is WeatherStationMeasurementType)
    """

    def __init__(self, interval, delay, time_field):
        self.__interval = interval
        self.__delay = delay
        self.__time_field = time_field
        self.__fields = {}

    @property
    def interval(self):
        return self.__interval

    @property
    def delay(self):
        return self.__delay

    @property
    def time_field(self):
        return self.__time_field

    @property
    def fields(self):
        return copy.deepcopy(self.__fields)

    def addField(self, field_name, field_type):
        self.__fields[field_name] = copy.deepcopy(field_type)


class TimeoffsetConfiguration:
    """This class holds the configuration parameters used when automatically determining the time offset
       between the timestamps in the database and the system clock.
        table         = the table that is used for the time offset calculations
        rounds        = how many update rounds are used to determine the time offset
        init_check    = in the initial round, the database is checked with this interval (in seconds)
        small_check   = when close to the update point, the database is checked with this interval (in seconds)
        round_check   = wait until this close (in seconds) to the update point before doing checks
        offset_growth = how much the time offset grows in seconds during one day
    """

    def __init__(self, table, rounds, init_check, small_check, round_check, offset_growth):
        self.__table = table
        self.__rounds = rounds
        self.__init_check = init_check
        self.__small_check = small_check
        self.__round_check = round_check
        self.__offset_growth = offset_growth

    @property
    def table(self):
        return self.__table

    @property
    def rounds(self):
        return self.__rounds

    @property
    def init_check(self):
        return self.__init_check

    @property
    def small_check(self):
        return self.__small_check

    @property
    def round_check(self):
        return self.__round_check

    @property
    def offset_growth(self):
        return self.__offset_growth


class WeatherStationDatabase:
    """This class holds the information about the different devices for the solar plant.
        address     = the address of the database
        port        = the access port of the database
        name        = the name of the database
        username    = the username for accessing the database
        password    = the password for the username
        time_offset = the time offset in seconds that should be added to the time values (defaults to 0.0)
        offset_conf = the configuration for the automatic time offset calculations
        tables      = a dict of the tables in the database (key is table_name, value is WeatherStationTable)
    """

    def __init__(self, address, port, name, username, password,
                 time_offset=datetime.timedelta(seconds=0.0), offset_conf=None):
        self.__address = address
        self.__port = port
        self.__name = name
        self.__username = username
        self.__password = password
        if type(time_offset) is not datetime.timedelta:
            self.__time_offset = datetime.timedelta(seconds=float(time_offset))
        else:
            self.__time_offset = time_offset
        self.__offset_conf = offset_conf
        self.__tables = {}

        self.__lock = threading.Lock()
        self.__offset_check_time = None

    @property
    def address(self):
        return self.__address

    @property
    def port(self):
        return self.__port

    @property
    def name(self):
        return self.__name

    @property
    def username(self):
        return self.__username

    @property
    def password(self):
        return self.__password

    @property
    def time_offset(self):
        return self.__time_offset

    @time_offset.setter
    def time_offset(self, new_offset):
        self.__time_offset = new_offset

    @property
    def offset_conf(self):
        return self.__offset_conf

    @property
    def tables(self):
        return copy.deepcopy(self.__tables)

    def addTable(self, table_name, table_type):
        self.__tables[table_name] = copy.deepcopy(table_type)

    def addField(self, table_name, field_name, field_type):
        if table_name in self.__tables:
            self.__tables[table_name].addField(field_name, field_type)
        else:
            print("ERROR: table", table_name, "not found, field", field_name, "not added.")

    def updateTimeoffset(self):
        with self.__lock:
            current_time = time.time()
            # only do offset time checking if it has been at least an hour since the last check
            if self.__offset_check_time is None or current_time - self.__offset_check_time >= 3600:
                postgres_utils.setTimeOffset(self)
                self.__offset_check_time = current_time

        return self.time_offset


class WeatherStationDataModel:
    """This class is used as a data model for the weather station databases.
         databases = a dict of added databases (key is database_id, value is WeatherStationDatabase)
    """

    def __init__(self):
        self.__databases = {}

    @property
    def databases(self):
        return self.__databases

    def addDatabase(self, database_id, database_type):
        self.__databases[database_id] = database_type

    def addField(self, database_id, table_name, field_name, field_type):
        if database_id in self.__databases:
            self.__databases[database_id].addField(table_name, field_name, field_type)
        else:
            print("ERROR: database", database_id, "not found, field", field_name, "not added to table", table_name)


def loadModel(config_filename, csv_filename):
    """Reads weather station measurement information from csv file using a json configuration file.
       Returns an object of WeatherStationDataModel class that can be used to access the measurement information.
    """

    try:
        # Load the configuration scheme from a json file.
        config = common_utils.readConfig(config_filename)

        # Add the databases to the model
        databases = config["databases"]
        data_model = WeatherStationDataModel()
        for database_id, database_info in databases.items():
            time_offset = database_info.get("time_offset_s", "")
            if time_offset == "":
                time_offset = datetime.timedelta(seconds=0.0)
            else:
                time_offset = datetime.timedelta(seconds=float(time_offset))

            offset_check = database_info.get("time_offset_check", False)
            if offset_check:
                offset_param = database_info["time_offset_parameters"]
                offset_conf = TimeoffsetConfiguration(
                    table=offset_param["time_check_table"],
                    rounds=offset_param["init_rounds"],
                    init_check=offset_param["init_check_s"],
                    small_check=offset_param["small_check_s"],
                    round_check=offset_param["round_check_s"],
                    offset_growth=offset_param["offset_growth_per_day_s"])
            else:
                offset_conf = None

            # Create a new database
            database = WeatherStationDatabase(
                address=database_info["address"],
                port=database_info["port"],
                name=database_info["name"],
                username=database_info["username"],
                password=database_info["password"],
                time_offset=time_offset,
                offset_conf=offset_conf)

            # Add the tables to the database
            tables = database_info["tables"]
            for table_name, table_info in tables.items():
                table = WeatherStationTable(
                    interval=table_info["interval_ms"],
                    delay=table_info["delay_ms"],
                    time_field=table_info["time_field"])

                database.addTable(table_name, table)

            data_model.addDatabase(int(database_id), database)

        csv_header = config["csv_header"]
        database_field = csv_header["database_id"]
        rtlid_field = csv_header["rtl_id"]
        table_field = csv_header["table"]
        field_field = csv_header["field"]
        name_field = csv_header["name"]
        path_field = csv_header["path"]
        unit_field = csv_header["unit"]
        datatype_field = csv_header["datatype"]
        ticket_field = csv_header["iot_ticket"]

        # Load the measurement information from a csv file.
        with open(csv_filename, mode="r") as csv_file:
            reader = csv.DictReader(csv_file, delimiter=";")

            for row in reader:
                if database_field in row:
                    database_id = int(row[database_field])
                else:
                    database_id = 1  # use 1 as a default value for database id
                table_name = row[table_field]
                field_name = row[field_field]

                field_type = WeatherStationMeasurementType(
                    name=row[name_field],
                    path=row[path_field],
                    rtl_id=int(row[rtlid_field]),
                    ticket=row[ticket_field] != "",
                    unit=row[unit_field],
                    data_type=row[datatype_field])
                data_model.addField(database_id, table_name, field_name, field_type)

    except Exception as error:
        # Something went wrong. Make it the problem of the caller.
        raise error

    return data_model
