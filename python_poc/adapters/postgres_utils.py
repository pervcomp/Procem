# -*- coding: utf-8 -*-
"""This module contains helper functions for working postgres database."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import psycopg2
import psycopg2.sql as sql
import datetime
import random
import time

try:
    import adapters.common_utils as common_utils
except:
    # used when running the module directly
    import common_utils

# SQL queries for getting the latest value of one field
init_query_string = "SELECT {field} FROM {table} ORDER BY {field} DESC LIMIT 1"
init_query_string_with_time = "SELECT {field} FROM {table} WHERE {field} > %s ORDER BY {field} DESC LIMIT 1"

# SQL query for getting the new values for specified fields
normal_query_string = "SELECT {fields} FROM {table} WHERE {time_field} > %s ORDER BY {time_field} ASC"


def getDatabaseAddress(database):
    """Returns the database address and port as a string."""
    return database.address + ":" + str(database.port)


def getConnection(database):
    """Returns a new connection to the database or None if it cannot be created."""
    try:
        connection = psycopg2.connect(
            dbname=database.name,
            host=database.address,
            port=database.port,
            user=database.username,
            password=database.password)
        return connection

    except Exception as error:
        print(common_utils.getTimeString(), "ERROR:", error)
        return None


def setTimeOffset(database, target_hour=12):
    """Sets and returns the time offset related to the given database.
       This function assumes that the database is updated with new data periodically
       with the same time interval each time and that time interval is not too large.
       If the function is called before the target hour, the calculated time offset is increased,
       so that the offset should be correct at the target hour.
    """

    # if no time offset configuration is given, just return the previously set value
    if database.offset_conf is None:
        return database.time_offset

    # The table used for the time offset checking
    table_name = database.offset_conf.table
    # The number of update rounds is used when finding the time offset
    rounds = database.offset_conf.rounds
    # How close to the database update point the small interval checks are used (in seconds)
    round_check = database.offset_conf.round_check
    # The small time interval that is used in determining the database update point (in seconds)
    small_check = database.offset_conf.small_check
    # What time interval is used in the first round for database checks (in seconds)
    init_check = database.offset_conf.init_check

    time_field = database.tables[table_name].time_field
    time_check_no_time_query = sql.SQL(init_query_string).format(
        field=sql.Identifier(time_field),
        table=sql.Identifier(table_name))
    time_check_with_time_query = sql.SQL(init_query_string_with_time).format(
        field=sql.Identifier(time_field),
        table=sql.Identifier(table_name))

    db_time = None
    time_offset = None

    try:
        connection = getConnection(database)
        if connection is None:
            print(common_utils.getTimeString(), "ERROR: Couldn't create connection to", getDatabaseAddress(database))
            return database.time_offset

        cursor = connection.cursor()
        round_count = 0
        new_offset = None
        min_offset = None
        max_offset = None

        print(common_utils.getTimeString(), "Checking for time offset in", getDatabaseAddress(database))
        while time_offset is None:
            if db_time is None:
                cursor.execute(time_check_no_time_query)
            else:
                cursor.execute(time_check_with_time_query, [db_time])

            result = cursor.fetchone()
            if result is not None:
                db_time = result[0]

            if db_time is not None:
                current_time = datetime.datetime.now()
                new_offset = (current_time - db_time).total_seconds()
                if min_offset is None or max_offset is None:
                    min_offset = new_offset
                    max_offset = new_offset
                elif new_offset < min_offset:
                    min_offset = new_offset
                elif new_offset > max_offset:
                    max_offset = new_offset

            if round_count >= rounds:
                # Use the smallest found offset rounded to the 1 ms range.
                offset_seconds = round(min_offset, 3)
                time_offset = datetime.timedelta(seconds=offset_seconds)
            else:
                if new_offset is not None and max_offset - new_offset > round_check:
                    round_count += 1
                    # print("After", round_count, "rounds, time offset is", min_offset)
                    # randomize the sleep time a bit
                    time.sleep(max_offset - new_offset - random.uniform(0.9*round_check, round_check))
                elif new_offset is None:
                    time.sleep(init_check)
                else:
                    time.sleep(small_check)

    except Exception as error:
        print(common_utils.getTimeString(), "ERROR: While checking for time offset for",
              getDatabaseAddress(database), ":", error)
        return database.time_offset

    # If the time offset check is done before the target hour, add to the time offset such that
    # the time offset will be correct at the target hour (assumes linear offset growth)
    current_time = time.localtime()
    if current_time.tm_hour < target_hour:
        to_target = datetime.timedelta(
            hours=target_hour - current_time.tm_hour - 1,
            minutes=60 - current_time.tm_min,
            seconds=60 - current_time.tm_sec)
        day = datetime.timedelta(days=1)
        growth_until_target = to_target / day * database.offset_conf.offset_growth
        time_offset += datetime.timedelta(seconds=growth_until_target)

    database.time_offset = time_offset
    print(common_utils.getTimeString(), "Time offset for", getDatabaseAddress(database), "set to ", end="")
    if time_offset.total_seconds() < 0:
        print("-", -time_offset, sep="")
    else:
        print(time_offset)
    return time_offset
