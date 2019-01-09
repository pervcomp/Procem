# -*- coding: utf-8 -*-
"""Module for reading and parsing values from HIRLAM weather forecast."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import datetime
import math
import requests
import time
import xml.etree.ElementTree

try:
    import adapters.common_utils as common_utils
    import adapters.rest_utils as rest_utils
except:
    # used when running the module directly
    import common_utils
    import rest_utils


def removeNamespace(tag):
    """Helper function for removing namespace part from an XML tag."""
    closing = tag.find("}")
    return tag[closing + 1:]


def getHirlamSimulationStartTime(root, datetime_format):
    """Helper function for getting the starting time for the latest simulation from HIRLAM XML-response."""
    try:
        # get the ending time by finding the tags "NamedValue" and "timePosition"
        result_time_elem = [elem for elem in root.iter() if "NamedValue" in elem.tag][0]
        time_elem = [elem for elem in result_time_elem.iter() if "timePosition" in elem.tag][0]
        start_time = datetime.datetime.strptime(
            time_elem.text, datetime_format).replace(tzinfo=datetime.timezone.utc).timestamp()
    except:
        return None

    return start_time


def getHirlamSimulationEndTime(root, datetime_format):
    """Helper function for getting the ending time for the latest simulation from HIRLAM XML-response."""
    try:
        # get the starting time by finding the tags "resultTime" and "timePosition"
        result_time_elem = [elem for elem in root.iter() if "resultTime" in elem.tag][0]
        time_elem = [elem for elem in result_time_elem.iter() if "timePosition" in elem.tag][0]
        end_time = datetime.datetime.strptime(
            time_elem.text, datetime_format).replace(tzinfo=datetime.timezone.utc).timestamp()
    except:
        return None

    return end_time


class Hirlam:
    """Class for getting and parsing data from HIRLAM weather forecast for a single location."""
    __META_FIELD = "SimulationTime"
    __COORDINATE_TOLERANCE = 0.001  # the response data is rejected if the coordinates are farther than this
    __PROCEM_SEND_WAIT = 0.01  # used to help with the data congestion when sending the data to Procem

    def __init__(self, params, data_queue):
        self.__config = params.get("config", {})
        self.__latitude = params.get("latitude", "")
        self.__longitude = params.get("longitude", "")
        self.__fields = params.get("fields", [])
        self.__start_hour = params.get("start_hour", 0)
        self.__end_hour = params.get("end_hour", 0)
        self.__prediction_time_step = params.get("time_step_minute", 60)

        self.__time_interval = params.get("time_interval_s", 3600)
        self.__time_interval_min = params.get("time_interval_min_s", 60)
        self.__rtl_id_base = int(params.get("rtl_id_base", 0))
        self.__path = params.get("iot_ticket_path_base", "/HIRLAM/{base:}")
        self.__name = params.get("iot_ticket_name_base", "{name:}")
        self.__desc = params.get("description", "{desc:}")

        self.__last_update = None
        self.__last_simulation_start_time = None
        self.__last_simulation_end_time = None
        self.__field_info = self.getHirlamFields()
        self.__data_queue = data_queue

        if self.__config.get("write_csv", False):
            self.writeHirlamCsv()

    def getHirlamFields(self):
        """Gets and stores the field information for HIRLAM forecast."""
        now = datetime.datetime.now()
        start_time = now.replace(hour=now.time().hour + 1, minute=0, second=0, microsecond=0)
        timestamp = start_time.timestamp()

        kwargs = {
            "config": self.__config,
            "start_time": timestamp,
            "end_time": timestamp,
            "fields": self.__fields
        }

        try:
            # run a simple API query to get the link for the field information
            req = rest_utils.runAPIQuery(**kwargs)

            if getattr(req, "status_code", 0) != rest_utils.STATUS_OK:
                return {}

            field_info_root = xml.etree.ElementTree.fromstring(req.text)

            datetime_format = self.__config.get("datetime_format", "")
            self.__last_simulation_start_time = getHirlamSimulationStartTime(field_info_root, datetime_format)

            # find the href for query for the field descriptions
            info_elem = [elem.attrib for elem in field_info_root.iter()
                         if removeNamespace(elem.tag) == "observedProperty"][0]
            href_id = [field for field in info_elem if removeNamespace(field) == "href"][0]

            # run the field description request
            description_req = requests.get(info_elem[href_id])

            if getattr(description_req, "status_code", 0) != rest_utils.STATUS_OK:
                return {}

            field_info = xml.etree.ElementTree.fromstring(description_req.text)

            # gather the field information (reserve one id number for simulation time)
            rtl_id = self.__rtl_id_base + 1
            fields = {}
            for item in field_info.iter():
                if removeNamespace(item.tag) != "ObservableProperty":
                    continue

                # find the name of the field
                id_attr = [attr for attr in item.attrib if removeNamespace(attr) == "id"][0]
                name = item.attrib[id_attr]
                fields[name] = {}

                fields[name]["rtl_id"] = rtl_id
                rtl_id += 1

                # find the other attributes for the field
                for elem in item:
                    if removeNamespace(elem.tag) == "label":
                        fields[name]["description"] = elem.text.strip()
                    elif removeNamespace(elem.tag) == "basePhenomenon":
                        fields[name]["base"] = elem.text.strip()
                    elif removeNamespace(elem.tag) == "uom":
                        fields[name]["unit"] = elem.attrib["uom"]

            return self.constructFieldInfo(fields)

        except Exception as error:
            print(common_utils.getTimeString(), "HIRLAM,", error)
            return {}

    def constructFieldInfo(self, fields):
        """Constructs and stores the field information in Procem compatible format."""
        field_info = {}

        # add the simulation time as a meta data field
        field_info[self.__META_FIELD] = {
            "rtl_id": self.__rtl_id_base,
            "datatype": "int",
            "unit": "ms",
            "name": self.__name.format(name=self.__META_FIELD),
            "path": self.__path.format(base="Time"),
            "confidential": False,
            "description": "The most recent simulation time"
        }

        for field_name, field_details in fields.items():
            unit = self.__config.get("replace_unit", {}).get(field_details["unit"], field_details["unit"])
            name = self.__config.get("replace_name", {}).get(field_name, field_name)

            field_info[field_name] = {
                "rtl_id": field_details["rtl_id"],
                "datatype": "float",
                "unit": unit,
                "name": self.__name.format(name=name),
                "path": self.__path.format(base=field_details["base"]),
                "confidential": False,
                "description": self.__desc.format(desc=field_details["description"])
            }

        return field_info

    def writeHirlamCsv(self):
        """Writes the data information to a CSV file."""
        try:
            delimiter = ";"

            filename = self.__config["csv_filename"]
            columns = [
                "rtl_id",
                "datatype",
                "unit",
                "name",
                "path",
                "confidential",
                "description"
            ]
            header = delimiter.join(columns)

            with open(filename, "w") as file:
                file.write(header + "\n")

                for field_name, field_details in self.__field_info.items():
                    rtl_id = str(field_details["rtl_id"])
                    datatype = field_details["datatype"]
                    unit = field_details["unit"]
                    name = field_details["name"]
                    path = field_details["path"]
                    if field_details["confidential"]:
                        confidential = "x"
                    else:
                        confidential = ""
                    desc = field_details["description"]

                    file.write(delimiter.join(
                        [rtl_id, datatype, unit, name, path, confidential, desc]) + "\n")

        except:
            print("Error while writing HIRLAM csv file.")

    def getData(self):
        """Tries to get new data from the HIRLAM API. If new data is found, it is send to the Procem RTL handler and
           the function returns True. Otherwise, the function returns False."""
        try:
            if self.__last_simulation_start_time is None:
                # NOTE: program gets here only if setting the simulation time failed during initialization
                now = datetime.datetime.now()
                start_time = now.replace(hour=now.time().hour + 1, minute=0, second=0, microsecond=0)
                start_timestamp = start_time.timestamp()
                end_timestamp = start_timestamp + self.__time_interval
            elif self.__last_update is None:
                # this is the first data gather attempt
                start_timestamp = self.__last_simulation_start_time + self.__start_hour * 3600
                end_timestamp = self.__last_simulation_start_time + self.__end_hour * 3600
            else:
                start_timestamp = self.__last_simulation_start_time + self.__time_interval + self.__start_hour * 3600
                end_timestamp = self.__last_simulation_start_time + self.__time_interval + self.__end_hour * 3600

            kwargs = {
                "config": self.__config,
                "latitude": self.__latitude,
                "longitude": self.__longitude,
                "start_time": start_timestamp,
                "end_time": end_timestamp,
                "fields": self.__fields,
                "time_step": self.__prediction_time_step
            }
            req = rest_utils.runAPIQuery(**kwargs)

            if req.status_code != rest_utils.STATUS_OK:
                print(common_utils.getTimeString(), "HIRLAM, received status code:", req.status_code)
                return False

            # the response is in XML format
            root = xml.etree.ElementTree.fromstring(req.text)

            datetime_format = self.__config.get("datetime_format", "")
            simulation_start_time = getHirlamSimulationStartTime(root, datetime_format)
            simulation_end_time = getHirlamSimulationEndTime(root, datetime_format)
            if (simulation_start_time is None or
                (self.__last_simulation_start_time is not None and
                    self.__last_update is not None and
                    simulation_start_time <= self.__last_simulation_start_time)):
                # no new data
                # print(common_utils.getTimeString(), "HIRLAM: no new data")
                return False

            position_data = self.getPositionData(root)
            value_data = self.getValueData(root)
            prediction_data = self.getPredictionData(position_data, value_data)

            self.sendDataToProcem(prediction_data, simulation_start_time)

            self.__last_simulation_start_time = simulation_start_time
            self.__last_simulation_end_time = simulation_end_time
            self.__last_update = time.time()
            return True

        except Exception as error:
            print(common_utils.getTimeString(), "ERROR: Hirlam,", error)
            return False

    def getPositionData(self, root):
        # Gets and returns the latitude, longitude and time for each prediction step
        position_data = []
        position_elems = [elem for elem in root.iter() if removeNamespace(elem.tag) == "SimpleMultiPoint"]
        for position_elem in position_elems:
            for positions in position_elem.iter():
                if removeNamespace(positions.tag) != "positions":
                    continue
                for position in positions.text.split("\n"):
                    data = list(filter(None, position.strip().split(" ")))
                    if len(data) == 3:
                        position_data.append({
                            "lat": float(data[0]),
                            "lon": float(data[1]),
                            "ts": int(data[2])})

        # all the positions should be the same, so just use the ones from the first element
        latitude = position_data[0]["lat"]
        longitude = position_data[0]["lon"]
        if (abs(latitude - self.__latitude) >= self.__COORDINATE_TOLERANCE or
                abs(longitude - self.__longitude) >= self.__COORDINATE_TOLERANCE):
            print(common_utils.getTimeString(), "HIRLAM, Received coordinates (", latitude, ", ", longitude,
                  ") when asking for (", self.__latitude, ", ", self.__longitude, ")", sep="")

        return position_data

    def getValueData(self, root):
        # Gets and returns the prediction values for each field and for each prediction step
        value_data = []
        value_elems = [elem for elem in root.iter() if removeNamespace(elem.tag) == "DataBlock"]
        for value_elem in value_elems:
            for values in value_elem.iter():
                if removeNamespace(values.tag) != "doubleOrNilReasonTupleList":
                    continue
                for value in values.text.split("\n"):
                    data = list(filter(None, value.strip().split(" ")))
                    if len(data) == len(self.__fields):
                        value_data.append([float(x) for x in data])

        return value_data

    def getPredictionData(self, position_data, value_data):
        """Combines the positional data with the prediction values and returns the prediction data."""
        if len(position_data) != len(value_data):
            print(common_utils.getTimeString(), "HIRLAM: different number of timestamps compared to prediction data")

        # combine prediction data with the timestamps
        prediction_data = []
        for values, position in zip(value_data, position_data):
            for index, value in enumerate(values):
                prediction_data.append({
                    "name": self.__fields[index],
                    "v": value,
                    "ts": position["ts"]})

        return prediction_data

    def getWaitingTime(self):
        """Returns the time in seconds that should be waited before making the next data query."""
        if self.__last_update is None or self.__last_simulation_end_time is None:
            return self.__time_interval_min / 2
        else:
            return max(
                self.__time_interval_min,
                self.__time_interval - self.__time_interval_min / 2 - (time.time() - self.__last_simulation_end_time))

    def sendDataToProcem(self, prediction_data, simulation_time):
        """Sends the prediction data to Procem RTL handler."""
        for data in prediction_data:
            v = data["v"]
            if math.isnan(v):
                continue

            field_name = data["name"]
            ts = int(data["ts"] * 1000)

            rtl_id = self.__field_info[field_name]["rtl_id"]
            name = self.__field_info[field_name]["name"]
            unit = self.__field_info[field_name]["unit"]
            datatype = self.__field_info[field_name]["datatype"]
            path = self.__field_info[field_name]["path"]
            confidential = self.__field_info[field_name]["confidential"]

            pkt_str = common_utils.getProcemRTLpkt(name, path, v, ts, unit, datatype, rtl_id, confidential)
            packet = bytes(pkt_str, "utf-8")
            self.__data_queue.put(packet)
            time.sleep(self.__PROCEM_SEND_WAIT)

        # send the simulation to Procem
        time_field = self.__field_info[self.__META_FIELD]
        v = int(simulation_time * 1000)
        ts = int(time.time() * 1000)
        rtl_id = time_field["rtl_id"]
        name = time_field["name"]
        unit = time_field["unit"]
        datatype = time_field["datatype"]
        path = time_field["path"]
        confidential = time_field["confidential"]

        pkt_str = common_utils.getProcemRTLpkt(name, path, v, ts, unit, datatype, rtl_id, confidential)
        packet = bytes(pkt_str, "utf-8")
        self.__data_queue.put(packet)

        # put empty item to the queue as a mark that the buffer should be emptied
        self.__data_queue.put(bytes())
