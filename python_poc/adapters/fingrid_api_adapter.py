# -*- coding: utf-8 -*-
"""Module for reading and parsing values from Fingrid APIs."""

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
import requests
import time

try:
    import adapters.common_utils as common_utils
    import adapters.rest_utils as rest_utils
except:
    # used when running the module directly
    import common_utils
    import rest_utils


class FingridCollection:
    """Class for holding a collection of Fingrid reader/handler objects."""
    def __init__(self, params, data_queue):
        # always wait at least this long before making a new query
        self.__min_waiting_time = params.get("min_waiting_time_s", 10)
        self.__fingrids = []  # the Fingrid objects
        self.__times = []  # the calculated waiting times for each Fingrid objects until next read should be done
        self.__last_check = time.time()  # the time in which the last API check was done
        self.__data_queue = data_queue  # the queue which is used to send the received data to Procem RTL handler
        self.createFingrids(params)

    def createFingrids(self, params):
        """Create the Fingrid objects for the collection according to the given parameters."""
        csv_filename = params.get("csv_filename", "")
        config = params.get("config", {})

        csv_header = config.get("csv_header", {})
        rtl_id_field = csv_header.get("rtl_id", "rtl_id")
        variable_id_field = csv_header.get("variable_id", "variable_id")
        datatype_field = csv_header.get("datatype", "datatype")
        unit_field = csv_header.get("unit", "unit")
        query_interval_field = csv_header.get("query_interval", "query_interval")
        query_interval_min_field = csv_header.get("query_interval_min", "query_interval_min")
        store_interval_field = csv_header.get("store_interval", "store_interval")
        is_prediction_field = csv_header.get("is_prediction", "is_prediction")
        prediction_length_field = csv_header.get("prediction_length", "prediction_length")
        name_field = csv_header.get("name", "name")
        path_field = csv_header.get("path", "path")
        confidential_field = csv_header.get("confidential", "confidential")

        try:
            with open(csv_filename, mode="r") as csv_file:
                reader = csv.DictReader(csv_file, delimiter=";")
                for row in reader:
                    new_params = copy.deepcopy(params)
                    new_params["rtl_id"] = int(row.get(rtl_id_field, 0))
                    new_params["id"] = int(row.get(variable_id_field, 0))
                    new_params["datatype"] = row.get(datatype_field, "float")
                    new_params["unit"] = row.get(unit_field, "")
                    new_params["time_interval_s"] = int(row.get(query_interval_field, 3600))
                    new_params["time_interval_min_s"] = int(row.get(query_interval_min_field, 60))
                    new_params["iot_ticket_name"] = row.get(name_field, "")
                    new_params["iot_ticket_path"] = row.get(path_field, "/Fingrid")
                    new_params["confidential"] = row.get(confidential_field, "") != ""

                    store_interval = row.get(store_interval_field, "")
                    if store_interval != "":
                        new_params["store_interval"] = int(store_interval)
                    is_prediction = row.get(is_prediction_field, "") != ""
                    if is_prediction:
                        new_params["is_prediction"] = is_prediction
                        new_params["prediction_length_s"] = int(row.get(prediction_length_field, 0))

                    self.__fingrids.append(Fingrid(new_params, self.__data_queue))
                    self.__times.append(None)
        except:
            pass

    def getData(self):
        """Tries to get new data from the Fingrid APIs. If new data is found, it is send to the Procem RTL handler and
           the function returns True. Otherwise, the function returns False."""
        time_diff = time.time() - self.__last_check

        success = []
        for index, (fingrid, waiting_time) in enumerate(zip(self.__fingrids, self.__times)):
            if waiting_time is None:
                self.__times[index] = fingrid.getWaitingTime() + time_diff
                continue
            elif waiting_time <= time_diff:
                success.append(fingrid.getData())
                self.__times[index] = None

        if success.count(True) > 0:
            # put empty item to the queue as a mark that the buffer should be emptied
            self.__data_queue.put(bytes())
            return True
        else:
            return False

    def getWaitingTime(self):
        """Returns the time in seconds that should be waited before making the next data query."""
        current_time = time.time()
        time_diff = current_time - self.__last_check

        for index, (fingrid, waiting_time) in enumerate(zip(self.__fingrids, self.__times)):
            if waiting_time is None:
                self.__times[index] = fingrid.getWaitingTime()
            else:
                self.__times[index] = max(waiting_time - time_diff, 0.0)

        min_waiting_time = min(self.__times)
        self.__last_check = current_time
        return max(min_waiting_time, self.__min_waiting_time)


class Fingrid:
    """Class for holding a single Fingrid API reader/handler."""
    def __init__(self, params, data_queue):
        self.__config = params.get("config", {})
        self.__variable_id = int(params.get("id", 0))
        self.__rtl_id = int(params.get("rtl_id", 0))
        self.__unit = params.get("unit", "")
        self.__datatype = params.get("datatype", "float")
        self.__path = params.get("iot_ticket_path", "/Fingrid")
        self.__name = params.get("iot_ticket_name", "")
        self.__confidential = params.get("confidential", False)

        self.__last_update = None  # the timestamp for the latest query time
        self.__last_value_dt = None  # the datetime for the latest received value
        self.__time_interval = params.get("time_interval_s", 3600)
        self.__time_interval_min = params.get("time_interval_min_s", 60)
        self.__store_interval = params.get("store_interval", 0)
        self.__is_prediction = params.get("is_prediction", False)
        self.__prediction_length = params.get("prediction_length_s", 0)

        self.__data_queue = data_queue

    def getStartTime(self):
        """Calculates and returns the start time as a timestamp for the next API query."""
        if self.__store_interval > 0:
            if self.__last_value_dt is not None and not self.__is_prediction:
                return (self.__last_value_dt + datetime.timedelta(seconds=self.__store_interval)).timestamp()

            dt_now = datetime.datetime.now().replace(microsecond=0)
            if self.__last_update is None:
                dt_now -= datetime.timedelta(seconds=self.__time_interval)

            day_start = dt_now.replace(hour=0, minute=0, second=0)
            seconds_back = int((dt_now - day_start).total_seconds()) % self.__store_interval
            dt_start = dt_now - datetime.timedelta(seconds=seconds_back)
            return dt_start.timestamp()

        else:
            if self.__last_update is None:
                return time.time() - self.__time_interval
            elif self.__is_prediction:
                return time.time()
            else:
                return self.__last_update + 1

    def getData(self):
        """Tries to get new data from the Fingrid API. If new data is found, it is send to the Procem RTL handler and
           the function returns True. Otherwise, the function returns False."""
        try:
            starttime = self.getStartTime()
            if self.__is_prediction:
                endtime = time.time() + self.__prediction_length
            else:
                endtime = time.time()

            # get the response from the API
            kwargs = {
                "config": self.__config,
                "variable_id": self.__variable_id,
                "start_time": starttime,
                "end_time": endtime
            }
            req = rest_utils.runAPIQuery(**kwargs)

            if req.status_code != rest_utils.STATUS_OK:
                print(common_utils.getTimeString(), "Fingrid, received status code:", req.status_code,
                      "for variable", self.__variable_id)
                return False

            result_datetime_format = self.__config["result_datetime_format"]
            data = json.loads(req.text)

            values = []
            first_dt = None
            if self.__is_prediction:
                self.__last_value_dt = None

            for item in data:
                v = item["value"]
                time_str = item["start_time"]
                dt = datetime.datetime.strptime(time_str, result_datetime_format)
                if (self.__last_value_dt is not None and
                        (dt - self.__last_value_dt).total_seconds() < self.__store_interval):
                    continue
                else:
                    self.__last_value_dt = dt
                    if first_dt is None:
                        first_dt = dt
                ts = int(dt.timestamp() * 1000)

                values.append({"v": v, "ts": ts})

            if len(values) == 0:
                return False

            self.sendDataToProcem(values)
            self.__last_update = time.time()
            return True

        except Exception as error:
            print(common_utils.getTimeString(), "Fingrid,", error)
            return False

    def getWaitingTime(self):
        """Returns the time in seconds that should be waited before making the next data query."""
        if self.__last_update is None:
            return self.__time_interval_min / 2
        else:
            return max(
                self.__time_interval_min,
                self.__time_interval - (time.time() - self.__last_update))

    def sendDataToProcem(self, values):
        """Sends the data to Procem RTL handler."""
        rtl_id = self.__rtl_id
        unit = self.__unit
        datatype = self.__datatype
        name = self.__name
        path = self.__path
        confidential = self.__confidential
        for item in values:
            v = item["v"]
            ts = item["ts"]

            pkt_str = common_utils.getProcemRTLpkt(name, path, v, ts, unit, datatype, rtl_id, confidential)
            packet = bytes(pkt_str, "utf-8")
            self.__data_queue.put(packet)
