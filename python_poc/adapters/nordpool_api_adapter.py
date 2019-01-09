# -*- coding: utf-8 -*-
"""Module for handling electricity SPOT price data from Nord Pool."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import datetime
import json
import time

try:
    import adapters.common_utils as common_utils
    import adapters.rest_utils as rest_utils
except:
    # used when running the module directly
    import common_utils
    import rest_utils


def timeToMicrosecondFormat(time_str, nzeros=6):
    """Helper function to change the given time string to a microsecond precision."""
    dot = time_str.rfind(".")
    if dot < 0:
        return time_str + "." + "0" * nzeros
    else:
        return time_str + "0" * (nzeros - (len(time_str) - dot - 1))


def isDSTTime(timestamp):
    """Helper function for determining whether the local time currently uses daylight saving time."""
    local_time = time.localtime(timestamp)
    return time.daylight > 0 and local_time.tm_isdst > 0


class Nordpool:
    """Class for receiving and handling data from Nord Pool."""
    def __init__(self, params, data_queue):
        self.__config = params.get("config", {})
        self.__currency = params.get("currency", "")
        self.__areas = params.get("areas", [])
        self.__last_query_date = None
        self.__last_update = None
        self.__time_interval = params.get("time_interval_s", 3600)
        self.__time_interval_min = params.get("time_interval_min_s", 60)
        self.__rtl_id_base = int(params.get("rtl_id_base", 0))
        self.__path = params.get("iot_ticket_path_base", "/{area_long:}")
        self.__name = params.get("iot_ticket_name_base", "{area:}_price")
        self.__desc = params.get("description", "{area_long:} price")

        # TODO: handle the clock changes better
        self.__from_dst_to_normal_days = [datetime.date(2018, 10, 28)]
        self.__clock_change_hour_utc = 0

        self.__data_info = self.getNordpoolInfo()
        self.__data_queue = data_queue

        if self.__config.get("write_csv", False):
            self.writeNordPoolCsv()

    def getNordpoolInfo(self):
        """Loads the data information using the given configurations."""
        try:
            # determine the unit by making a request to the Nord Pool API
            kwargs = {
                "config": self.__config,
                "currency": self.__currency,
                "date": time.time()
            }
            req = rest_utils.runAPIQuery(**kwargs)
            if req.status_code == rest_utils.STATUS_OK:
                js = json.loads(req.text)
                unit = js["data"]["Units"][0]
            else:
                unit = ""

            # collect the data information for each considered area.
            info = {}
            count = 0
            for area in self.__areas:
                info[area] = {}
                count += 1
                area_long = self.__config.get("long_names", {}).get(area, area)

                info[area]["rtl_id"] = self.__rtl_id_base + count
                info[area]["name"] = self.__name.format(area=area)
                info[area]["path"] = self.__path.format(area_long=area_long)
                info[area]["unit"] = unit
                info[area]["datatype"] = "float"
                info[area]["confidential"] = False
                info[area]["description"] = self.__desc.format(area_long=area_long)

            return info

        except:
            return {}

    def writeNordPoolCsv(self):
        """Writes the data information to a CSV file."""
        try:
            delimiter = ";"

            filename = self.__config["csv_filename"]
            columns = [
                "rtl_id",
                "area",
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

                for area, area_info in self.__data_info.items():
                    rtl_id = str(area_info["rtl_id"])
                    datatype = area_info["datatype"]
                    unit = area_info["unit"]
                    name = area_info["name"]
                    path = area_info["path"]
                    if area_info["confidential"]:
                        confidential = "x"
                    else:
                        confidential = ""
                    desc = area_info["description"]

                    file.write(delimiter.join([rtl_id, area, datatype, unit, name, path, confidential, desc]) + "\n")

        except:
            print(common_utils.getTimeString(), "Error while writing Nord Pool csv file.")

    def getData(self):
        """Tries to get new data from the Nord Pool API. If new data is found, it is send to the Procem RTL handler and
           the function returns True. Otherwise, the function returns False."""

        try:
            if self.__last_query_date is None:
                timestamp = time.time()
            else:
                timestamp = (self.__last_query_date + datetime.timedelta(days=1)).timestamp()
            kwargs = {
                "config": self.__config,
                "currency": self.__currency,
                "date": timestamp
            }
            req = rest_utils.runAPIQuery(**kwargs)

            if req.status_code != rest_utils.STATUS_OK:
                print(common_utils.getTimeString(), "Nord Pool, received status code:", req.status_code)
                return False

            js = json.loads(req.text)

            if self.__currency != js["currency"]:
                print(common_utils.getTimeString(), "Nord Pool, received currency:", js["currency"])
                return False

            data = js["data"]

            # use the time zone field in the response to get the proper timestamps
            # NOTE: changes to hardcoded timezone since the website started giving wrong timezone on 2018-10-28
            time_zone_info = 1
            # time_zone_info = int(data["TimeZoneInformation"])
            if isDSTTime(timestamp) or datetime.date.fromtimestamp(timestamp) in self.__from_dst_to_normal_days:
                time_zone_info += 1
            timezone = datetime.timezone(datetime.timedelta(hours=time_zone_info))

            result_datetime_format = self.__config["result_datetime_format"]
            update_time_str = timeToMicrosecondFormat(data["DateUpdated"])
            update_date = datetime.datetime.strptime(update_time_str, result_datetime_format).replace(tzinfo=timezone)
            update_timestamp = update_date.timestamp()

            price_data = self.getPriceData(data["Rows"], timezone)
            received_prices = [len(prices) for area, prices in price_data.items()]

            if datetime.date.fromtimestamp(timestamp) in self.__from_dst_to_normal_days:
                hour_count = 25
            else:
                hour_count = 24
            if max(received_prices) != hour_count or min(received_prices) != hour_count:
                print(common_utils.getTimeString(), " Nord Pool: ", max(received_prices), "/", hour_count,
                      " prices received.", sep="")
                return False

            self.sendDataToProcem(price_data)

            self.__last_update = update_timestamp
            self.__last_query_date = datetime.datetime.fromtimestamp(timestamp).replace(
                hour=12, minute=0, second=0, microsecond=0)
            return True

        except Exception as error:
            print(common_utils.getTimeString(), "Nord Pool:", error)
            return False

    def getWaitingTime(self):
        """Returns the time in seconds that should be waited before making the next data query."""
        if self.__last_update is None or self.__last_query_date is None:
            # no data received yet at all
            return self.__time_interval_min / 2
        elif self.__last_query_date.day == datetime.datetime.fromtimestamp(self.__last_update).day:
            # last data query was for today, try to get tomorrows data as soon as possible
            return self.__time_interval_min
        else:
            return max(
                self.__time_interval_min,
                self.__time_interval - (time.time() - self.__last_update) + self.__time_interval_min / 2)

    def getPriceData(self, rows, timezone):
        """Parses the price data from the given response data."""
        clock_changed = False

        price_data = {}
        for row in rows:
            if row["IsExtraRow"]:
                continue

            result_datetime_format = self.__config["result_datetime_format"]
            start_time_str = timeToMicrosecondFormat(row["StartTime"])
            dt = datetime.datetime.strptime(start_time_str, result_datetime_format).replace(
                tzinfo=timezone).astimezone(datetime.timezone.utc)
            ts = int(dt.timestamp() * 1000)

            if (dt.date() in self.__from_dst_to_normal_days and
                    not clock_changed and
                    dt.hour == self.__clock_change_hour_utc):
                timezone = datetime.timezone(timezone.utcoffset(dt) - datetime.timedelta(hours=1))
                clock_changed = True

            for area in self.__areas:
                if area not in price_data:
                    price_data[area] = []

                column = [column for column in row["Columns"] if column["Name"] == area][0]
                if not column["IsValid"] or not column["IsOfficial"] or column["Value"] == "-":
                    continue

                value = float(column["Value"].replace(",", "."))
                price_data[area].append({
                    "v": value,
                    "ts": ts
                })

        return price_data

    def sendDataToProcem(self, price_data):
        """Sends the price data to Procem RTL handler."""
        for area, values in price_data.items():
            rtl_id = self.__data_info[area]["rtl_id"]
            unit = self.__data_info[area]["unit"]
            datatype = self.__data_info[area]["datatype"]
            name = self.__data_info[area]["name"]
            path = self.__data_info[area]["path"]
            confidential = self.__data_info[area]["confidential"]
            for value in values:
                v = value["v"]
                ts = value["ts"]

                pkt_str = common_utils.getProcemRTLpkt(name, path, v, ts, unit, datatype, rtl_id, confidential)
                packet = bytes(pkt_str, "utf-8")
                self.__data_queue.put(packet)

        # put empty item to the queue as a mark that the buffer should be emptied
        self.__data_queue.put(bytes())
