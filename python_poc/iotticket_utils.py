# -*- coding: utf-8 -*-
"""This module contains helper functions for data transfer to Iot-Ticket.
   Code is based on https://github.com/IoT-Ticket/IoTTicket-PythonLibrary
"""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

# The MIT License (MIT)
#
# Copyright (c) 2016 Wapice Ltd.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import adapters.common_utils as common_utils
import requests
import time

STATUS_OK = 200
CREATED = 201
INSUFFICIENT_PERMISSION = 8001


class SimpleIoTTicketClient:
    """A simple IoT-Ticket client that can be used to read and write data to IoT-Ticket."""
    readdataresource = "process/read/{}/"
    writedataresource = "process/write/{}/"
    deviceresource = "devices"
    datanoderesource = "devices/{}/datanodes"

    def __init__(self, base_url, username, password):
        self.__base_url = base_url
        self.__auth = requests.auth.HTTPBasicAuth(username, password)

    def registerDevice(self, device):
        """Registers a device."""
        url = self.__base_url + self.deviceresource
        req = requests.post(url, json=device, auth=self.__auth)
        resp = getResponce(req)
        return resp

    def getDevices(self):
        """Return all the devices as a list."""
        LIMIT = 100  # maximum number of devices in a single request

        responces = []
        current_offset = 0
        try:
            session = requests.session()
            session.auth = self.__auth
            url = self.__base_url + self.deviceresource
            url += "?limit={limit:d}".format(limit=LIMIT)
            url += "&offset={offset:d}"
            while True:
                req = session.get(url.format(offset=current_offset))
                resp = getResponce(req)
                responces.append(resp)

                # stop if responce doesn't have status ok
                if resp is None or resp.get("status_code", 0) != STATUS_OK:
                    break

                # if there is less than 100 devices, we are at the end of the full list
                if len(resp.get("content", {}).get("items", [])) < LIMIT:
                    break
                else:
                    current_offset += LIMIT
        except:
            # TODO: add error handling
            pass

        return combineListResponces(responces)

    def getDatanodes(self, device_id):
        """Return all the datanodes for the device as a list."""
        LIMIT = 100  # maximum number of datanodes in a single request

        responces = []
        current_offset = 0
        try:
            session = requests.session()
            session.auth = self.__auth
            url = self.__base_url + self.datanoderesource.format(device_id)
            url += "?limit={limit:d}".format(limit=LIMIT)
            url += "&offset={offset:d}"
            while True:
                req = session.get(url.format(offset=current_offset))
                resp = getResponce(req)
                responces.append(resp)

                # stop if responce doesn't have status ok
                if resp is None or resp.get("status_code", 0) != STATUS_OK:
                    break

                # if there is less than 100 datanodes, we are at the end of the full list
                if resp.get("content", {}).get("fullSize", 0) < LIMIT:
                    break
                else:
                    current_offset += LIMIT
        except:
            # TODO: add error handling
            pass

        return combineListResponces(responces)

    def readData(self, device_id, datanodes, fromdate=None, todate=None, limit=None):
        """Reads datanodes from a device. Returns all results from the chosen time period."""
        start_limit = 10000  # maximum number of measurements per datanode for a single request

        if todate is None:
            todate = int(time.time() * 1000)
        if type(datanodes) is str:
            datanodes = [datanodes]

        responces = []
        try:
            session = requests.session()
            session.auth = self.__auth

            # construct the url for the query
            full_url = self.__base_url + self.readdataresource.format(device_id)
            full_url += "?datanodes={datanode}"
            if fromdate is not None:
                full_url += "&fromdate={fromdate:d}"
                full_url += "&todate=" + str(todate)
                if limit is not None:
                    start_limit = min(start_limit, limit)
                    full_url += "&limit={limit:d}"
                else:
                    full_url += "&limit={limit:d}".format(limit=start_limit)

            # loop through the datanodes, each datanode is queried separately
            # (up to 10 datanodes could be queried at the same time)
            for datanode in datanodes:
                current_fromdate = fromdate
                current_limit = start_limit
                n_values = 0  # the total number of values received so far

                continue_reading = True
                while continue_reading:
                    if fromdate is None:
                        current_url = full_url.format(datanode=datanode)
                    elif limit is None:
                        current_url = full_url.format(datanode=datanode, fromdate=current_fromdate)
                    else:
                        current_url = full_url.format(datanode=datanode, fromdate=current_fromdate, limit=current_limit)

                    req = session.get(current_url)
                    resp = getResponce(req)
                    responces.append(resp)

                    # stop if the responce doesn't have status ok
                    if resp is None or resp.get("status_code", 0) != STATUS_OK:
                        break

                    # stop if no datanodes are found in the responce
                    node_reads = resp.get("content", {}).get("datanodeReads", [])
                    if len(node_reads) == 0:
                        break

                    # get the maximum number of measurements for a datanode in the responce
                    max_reads = max([len(node.get("values", [])) for node in node_reads])
                    if max_reads < current_limit or fromdate is None:
                        break
                    n_values += max_reads
                    if limit is not None:
                        current_limit = min(limit - n_values, current_limit)

                    # get the largest timestamp in the results
                    max_ts = max([value.get("ts", 0) for node in node_reads for value in node.get("values", [])])
                    current_fromdate = max_ts + 1

                    continue_reading = current_fromdate <= todate and current_limit > 0

        except:
            # TODO: add error handling
            pass

        return combineReadResponces(responces)

    def writeData(self, device_id, jsondata, packet_size=None, considered_packets=None):
        """Writes data to IoT-Ticket and returns a list of responces.
           The jsondata is expected to contain valid data as a list of json objects.
           Using packet_size parameter the data sending can be serialized to smaller packets.
           Using considered_packets (containing packet numbers) only part of the data can be sent."""
        path_url = self.__base_url + self.writedataresource.format(device_id)

        # the send will be done as one packet
        if packet_size is None or len(jsondata) <= packet_size:
            if considered_packets is not None and 0 not in considered_packets:
                return [None]
            try:
                req = requests.post(path_url, json=jsondata, auth=self.__auth)
                resp = getResponce(req)
                return [resp]

            except requests.exceptions.RequestException as error:
                error.responces = [None]
                raise error

        # the send will be divided into two or more packets
        json_packets = list(common_utils.chunks(jsondata, packet_size))
        if considered_packets is None:
            considered_packets = set(range(len(json_packets)))

        responces = []
        try:
            session = requests.session()
            session.auth = self.__auth

            for index, json_packet in enumerate(json_packets):
                if index in considered_packets:
                    new_req = session.post(path_url, json=json_packet)
                    new_resp = getResponce(new_req)
                    responces.append(new_resp)
                else:
                    responces.append(None)

        except requests.exceptions.RequestException as error:
            responces += [None] * (len(json_packets) - len(responces))
            error.responces = responces
            raise error
        else:
            return responces


def getResponce(req):
    """Returns a http responce from a http request. The responce is expected to be in json format."""
    try:
        resp = {
            "status_code": req.status_code,
            "encoding": req.encoding,
            "headers": req.headers,
            "url": req.url,
            "content": req.json()
        }
    except:
        return None
    else:
        return resp


def combineListResponces(responces):
    """Combines a list of device or datanode responces and returns the combined results."""
    responces = [resp for resp in responces if resp is not None]

    items = []
    for responce in responces:
        for item in responce.get("content", {}).get("items", []):
            items.append(item)
    return items


def combineReadResponces(responces):
    """Combines a list of read data responces and returns the combined results."""
    responces = [resp for resp in responces if resp is not None]
    if len(responces) == 0:
        return None
    elif len(responces) == 1:
        return responces[0]

    hrefs = []
    fullnames = []
    datanode_reads = []

    for responce in responces:
        if responce.get("status_code", 0) != STATUS_OK:
            continue
        content = responce.get("content", {})
        hrefs.append(content.get("href", ""))
        datanodes = content.get("datanodeReads", [])
        for datanode in datanodes:
            name = datanode.get("name", None)
            if name is None:
                continue
            path = datanode.get("path", None)
            if path is None:
                fullname = name
            else:
                fullname = path + "/" + name

            if fullname in fullnames:
                index = fullnames.index(fullname)
            else:
                fullnames.append(fullname)
                index = len(fullnames) - 1

                new_datanode = {}
                for attr_name, attr_value in datanode.items():
                    if attr_name != "values":
                        new_datanode[attr_name] = attr_value
                new_datanode["values"] = []
                datanode_reads.append(new_datanode)

            datanode_reads[index]["values"] += datanode.get("values", [])

    total_content = {
        "href": hrefs,
        "datanodeReads": datanode_reads
    }

    combined_responce = {
        "status_code": [resp.get("status_code", 0) for resp in responces],
        "encoding": [resp.get("encoding", "") for resp in responces],
        "headers": [resp.get("headers", {}) for resp in responces],
        "url": [resp.get("url", "") for resp in responces],
        "content": total_content
    }

    return combined_responce


def getResponceInfo(responces, n_measurements, packet_size, considered_packets):
    """Analyses the responces from a IoT-Ticket write call.
         responces          = the responces received from the function writeData
         n_measurements     = the total number of measurements send to IoT-Ticket
         packet_size        = the maximum number of measurements in a single packet
         considered_packets = a set of indexes of the packets that were sent, any packet
                              that were successfully sent is removed from the set
       Returns the total number of measurements that were successfully sent and
       a flag that tells if there should be extra wait time before trying a resend.
    """
    confirmed_written = 0
    extra_wait = False
    status_codes = []
    iott_codes = []
    total_written = []
    for resp in responces:
        if resp is None:
            status_codes.append(None)
            iott_codes.append(None)
            total_written.append(0)
        else:
            status_codes.append(resp.get("status_code", "unknown"))
            iott_codes.append(resp.get("content", {}).get("code", "unknown"))
            total_written.append(resp.get("content", {}).get("totalWritten", 0))

    for index, (status_code, iott_code, total) in enumerate(zip(status_codes, iott_codes, total_written)):
        if index not in considered_packets:
            continue

        confirmed_written += total
        if status_code == STATUS_OK or status_code == CREATED:
            if index < len(responces) - 1:
                total_max = packet_size
            else:
                total_max = n_measurements % packet_size
                if total_max == 0:
                    total_max = packet_size

            if total == total_max:
                # successful send
                considered_packets.remove(index)
            elif total > 0:
                # Some data was send but not all
                # NOTE: The responce could be parsed to get more information but only counts per datanodes
                # are available. For now, just forget about the missed measurements
                print(common_utils.getTimeString(), " IOTT write with ", total, "/", total_max, sep="")
                considered_packets.remove(index)
            else:
                # the connection was successful but no measurements were sent
                pass

        else:  # a failed send
            if iott_code == INSUFFICIENT_PERMISSION:
                # extra long wait before resend because the error code corresponds to unauthorized access
                if not extra_wait:
                    print(common_utils.getTimeString(), "received iott_code:", iott_code)
                    extra_wait = True

    return confirmed_written, extra_wait
