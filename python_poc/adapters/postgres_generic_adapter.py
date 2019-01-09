# -*- coding: utf-8 -*-
"""This module includes the adapter for reading periodically updated values from a PostgreSQL database
   and sending the values to the Procem RTL worker for further handling."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import psycopg2.sql as sql
import datetime
import queue
import random
import sys
import threading
import time

try:
    import adapters.common_utils as common_utils
    import adapters.postgres_utils as postgres_utils
    import adapters.postgres_weatherstation_model as model
except:
    # used when running the module directly
    import common_utils
    import postgres_utils
    import postgres_weatherstation_model as model

PROCEM_SERVER_IP = common_utils.PROCEM_SERVER_IP
PROCEM_SERVER_PORT = common_utils.PROCEM_SERVER_PORT

# maximum size for UDP payload. Current value based on a quick experiment where it was 8192
UDP_MAX_SIZE = common_utils.UDP_MAX_SIZE

# The names of the configuration files from where the data model information is read
CONFIG_SCHEME_FILE_NAME = "postgres_weatherstation_config.json"
MEASUREMENT_ID_FILE_NAME = "Wheather_Station_measurement_IDs.csv"

init_query_string = postgres_utils.init_query_string
normal_query_string = postgres_utils.normal_query_string


def PostgresTableWorker(connection, table_name, table_type, database, data_queue):
    """Reads values periodically from the given table and sends to Procem RTL worker."""
    try:
        cursor = connection.cursor()

        table_sql = sql.Identifier(table_name)
        time_field_sql = sql.Identifier(table_type.time_field)

        # Gather the field names to variable field_names_sql
        field_names = []
        for field_name in table_type.fields:
            field_names.append(sql.Identifier(field_name))
        if len(field_names) == 0:
            print(common_utils.getTimeString(), "No fields specified for table", table_name, "in database",
                  postgres_utils.getDatabaseAddress(database))
            quit()
        field_names_sql = sql.SQL(", ").join([time_field_sql] + field_names)

        # get the latest database entry time by using an initial query
        latest_time = None
        while latest_time is None:
            cursor.execute(sql.SQL(init_query_string).format(field=time_field_sql, table=table_sql))
            result = cursor.fetchone()
            if result is not None:
                latest_time = result[0]
            else:
                time.sleep(table_type.delay / 1000)

        # Construct the query that will be used to get the data
        query = sql.SQL(normal_query_string).format(
            fields=field_names_sql,
            table=sql.Identifier(table_name),
            time_field=sql.Identifier(table_type.time_field))

        time_offset = database.time_offset
        loop_start_time = time.time()
        last_save_time = latest_time
        day = datetime.date.today().day

        packet_count = 0
        print_interval = 3600  # print the number of sent packages once in an hour
        next_print_count = print_interval
        while True:
            # Run the time offset checking if it is a new day.
            current_day = datetime.date.today().day
            if current_day != day:
                time_offset = database.updateTimeoffset()
                day = current_day

            time_until_next_query = max(table_type.delay / 1000 - (time.time() - loop_start_time), 0)
            time.sleep(time_until_next_query)

            cursor.execute(query, [latest_time])
            results = cursor.fetchall()
            loop_start_time = time.time()

            for result in results:
                latest_time = result[0]
                time_interval = (latest_time - last_save_time) / datetime.timedelta(milliseconds=1)

                if time_interval > table_type.interval:
                    last_save_time = latest_time
                    ts = int(round((latest_time + time_offset).timestamp() * 1000))
                    for field_name, value in zip(table_type.fields, result[1:]):
                        field_info = table_type.fields[field_name]
                        new_pkt = bytes(common_utils.getProcemRTLpkt(
                            name=field_info.name,
                            path=field_info.path,
                            value=value,
                            timestamp=ts,
                            unit=field_info.unit,
                            datatype=field_info.data_type,
                            variableNumber=field_info.rtl_id,
                            confidential=not field_info.ticket), "utf-8")
                        data_queue.put(new_pkt)

                    packet_count += 1

            # put empty item into the data queue as a mark that the buffer should be emptied
            data_queue.put(bytes())

            if packet_count >= next_print_count:
                print(common_utils.getTimeString(), packet_count, "packages sent from", table_name)
                next_print_count += print_interval

    except Exception as error:
        print(common_utils.getTimeString(), " ERROR: Table: ", table_name, ", Message: ", error, sep="")

        # try to create a new connection and start the table worker again
        time.sleep(2 * table_type.delay / 1000)
        connection = postgres_utils.getConnection(database)

        print(common_utils.getTimeString(), " (", postgres_utils.getDatabaseAddress(database),
              ") starting thread for table: ", table_name, sep="")
        table_thread = threading.Thread(
            target=PostgresTableWorker,
            kwargs={
                "connection": connection,
                "table_name": table_name,
                "table_type": table_type,
                "database": database,
                "data_queue": data_queue},
            daemon=True)
        table_thread.start()


def PostgresDatabaseWorker(database, data_queue):
    """Finds the time offset for the database and starts a new thread for each table in the database."""
    postgres_utils.setTimeOffset(database)
    for table_name, table in database.tables.items():
        try:
            connection = postgres_utils.getConnection(database)

            print(common_utils.getTimeString(), " (", postgres_utils.getDatabaseAddress(database),
                  ") starting thread for table: ", table_name, sep="")
            table_thread = threading.Thread(
                target=PostgresTableWorker,
                kwargs={
                    "connection": connection,
                    "table_name": table_name,
                    "table_type": table,
                    "database": database,
                    "data_queue": data_queue},
                daemon=True)
            table_thread.start()

        except Exception as error:
            print(common_utils.getTimeString(), "ERROR:", error)


def startPostgresAdapter(data_model, data_queue):
    """Starts a new thread for each database in the data model."""
    databases = data_model.databases
    for database_id, database in databases.items():
        print(common_utils.getTimeString(), " Starting thread for database ", database_id, ": ",
              postgres_utils.getDatabaseAddress(database), sep="")
        postgres_thread = threading.Thread(
            target=PostgresDatabaseWorker,
            kwargs={"database": database, "data_queue": data_queue},
            daemon=True)
        postgres_thread.start()


if __name__ == "__main__":
    if len(sys.argv) == 3:
        CONFIG_SCHEME_FILE_NAME = sys.argv[1]
        MEASUREMENT_ID_FILE_NAME = sys.argv[2]
    elif len(sys.argv) != 1:
        print("Start this adapter with 'python3", sys.argv[0], "config_scheme.json measurement_ids.csv' command")
        print("or use 'python3 ", sys.argv[0], "' to use the default configuration.", sep="")
        quit()

    # read configuration information from the configuration files
    print(common_utils.getTimeString(), "Reading configurations from",
          CONFIG_SCHEME_FILE_NAME, "and", MEASUREMENT_ID_FILE_NAME)
    data_model = model.loadModel(
        config_filename=CONFIG_SCHEME_FILE_NAME,
        csv_filename=MEASUREMENT_ID_FILE_NAME)

    # initialize the data queue and start the udp send thread
    data_queue = queue.Queue()
    threading.Thread(target=common_utils.procemSendWorker, kwargs={"data_queue": data_queue}).start()

    startPostgresAdapter(data_model, data_queue)

    while True:
        txt = input("Press enter key to end:\n\r")
        if not txt:
            break
