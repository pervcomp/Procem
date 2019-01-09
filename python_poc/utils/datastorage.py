# -*- coding: utf-8 -*-
"""This module contains a thread-safe storage class for holding collection values with timestamps."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import bisect
import threading


class ValueTs:
    """Class for holding values with timestamps."""
    def __init__(self, value, timestamp):
        self.v = value
        self.ts = timestamp

    def __eq__(self, other):
        return self.v == other.v and self.ts == other.ts

    def __lt__(self, other):
        return self.ts < other.ts or (self.ts == other.ts and self.v < other.v)

    def __repr__(self):
        return str((self.v, self.ts))

    def __str__(self):
        return str((self.v, self.ts))


class DataStorage:
    """A class for holding a thread-safe collection of data values with timestamps.
       Only a limited number of the most recent values for each measurement will be stored."""
    def __init__(self, limit=1):
        self.__data = {}
        self.__read_lock = threading.Lock()
        self.__write_lock = threading.Lock()
        self.__limit = limit
        self.__individual_limits = {}

    def set_id_limit(self, rtl_id, limit):
        """Sets an individual id specific maximum limit for the number of values.
           The default value for the maximum limit is set in the constructor."""
        with self.__write_lock:
            self.__individual_limits[rtl_id] = limit
            with self.__read_lock:
                if rtl_id in self.__data and len(self.__data[rtl_id]) > limit:
                    self.__data[rtl_id][:] = self.__data[rtl_id][-limit:]

    def add_value(self, rtl_id, value, timestamp):
        """Adds a new value to the data collection."""
        new_value = ValueTs(value, timestamp)
        with self.__write_lock:
            if rtl_id in self.__individual_limits:
                limit = self.__individual_limits[rtl_id]
            else:
                limit = self.__limit

            if rtl_id not in self.__data:
                # add a new id to the data collection
                with self.__read_lock:
                    self.__data[rtl_id] = [new_value]
            elif new_value not in self.__data[rtl_id]:
                index, is_old_item = self.get_index(rtl_id, timestamp)
                with self.__read_lock:
                    if is_old_item:
                        # replace the old value with the new one
                        self.__data[rtl_id][index] = new_value
                    else:
                        # add the new item to the list and drop the oldest value if necessary
                        self.__data[rtl_id].insert(index, new_value)
                        if len(self.__data[rtl_id]) > limit:
                            self.__data[rtl_id][:] = self.__data[rtl_id][-limit:]

    def get_ids(self):
        """Returns the ids that have values in the collection."""
        with self.__read_lock:
            return sorted(self.__data.keys())

    def get_value(self, rtl_id):
        """Returns the most recent value for the given id."""
        values = self.get_values(rtl_id, 1)
        if values is None or len(values) < 1:
            return None
        else:
            return values[-1]

    def get_values(self, rtl_id, max_values=None):
        """Returns the values for the given id.
           If max_value is set, returns a maximum of max_values of the most recent values."""
        with self.__read_lock:
            if rtl_id not in self.__data:
                return None
            else:
                if max_values is None:
                    return self.__data[rtl_id]
                else:
                    return self.__data[rtl_id][-max_values:]

    def get_index(self, rtl_id, timestamp):
        """If an item with the given id and timestamp exists, returns (index, True), where index corresponds to the
           item. Otherwise, returns (index, False), where index corresponds to the place in the list where the new
           item should go to."""
        with self.__read_lock:
            if rtl_id not in self.__data or len(self.__data[rtl_id]) == 0:
                return 0, False

            items = self.__data[rtl_id]
            index = bisect.bisect_left(items, ValueTs(0, timestamp))
            if index < len(items) and items[index].ts == timestamp:
                return index, True
            elif index < len(items)-1 and items[index+1].ts == timestamp:
                return index + 1, True
            elif index > 0 and items[index-1].ts == timestamp:
                return index - 1, True
            else:
                return index, False
