# -*- coding: utf-8 -*-
"""This module contains helper function for registering a device for Iot-Ticket."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import iotticket_utils
import json


def registerNewDevice(conf, name, manufacturer, devtype="", desc="", attributes=None):
    if attributes is None:
        attributes = []
    conffile = open(conf, "r")
    jd = json.load(conffile)

    device = {
        "name": name,
        "manufacturer": manufacturer
    }

    # These are optional
    if devtype != "":
        device["type"] = devtype
    if desc != "":
        device["description"] = desc
    if len(attributes) > 0:
        device["attributes"] = []
        for attribute in attributes:
            key = attribute[0].strip()
            value = attribute[1].strip()
            device["attributes"].append({"key": key, "value": value})

    print("The device parameters are as follows:")
    for key, value in device.items():
        if key != "attributes":
            print("  ", key, ": ", value, sep="")
        else:
            print("  ", key, ": ", value[0]["key"], ": ", value[0]["value"], sep="")
            for index in range(1, len(value)):
                print("  ", " "*len(key), "  ", value[index]["key"], ": ", value[index]["value"], sep="")
    answer = input("Are these correct (yes/no)? ")

    if answer.lower() == "yes" or answer.lower() == "y":
        client = iotticket_utils.SimpleIoTTicketClient(jd["baseurl"], jd["username"], jd["password"])
        resp = client.registerDevice(device)
        deviceId = resp["content"]["deviceId"]
        return deviceId
    else:
        return ""


if __name__ == "__main__":
    print("This is interactive deviceID creator for IoTTicket.")
    print("To skip an option/question, input empty string (just press enter).")
    print()
    conffile = input("The filename of your configuration: ")
    deviceName = input("The name of the device [REQ]: ")
    deviceManuf = input("The manufacturer of the device [REQ]: ")
    deviceType = input("The type of the device [OPT]: ")
    deviceDescription = input("The description of the device [OPT]: ")

    maxAttributes = 50
    deviceAttributeKeyList = []
    deviceAttributeValueList = []
    print("[OPT] Additional attributes as (key, value) pairs (key and value given separately):",
          "e.g. 'version' and '1.0'")
    print("A maximum of", maxAttributes, "attributes is allowed. To skip, input empty string.")
    while len(deviceAttributeKeyList) < maxAttributes:
        text = input("[OPT] attribute {:d}, key: ".format(len(deviceAttributeKeyList)+1))
        if text == "":
            break
        deviceAttributeKeyList.append(text)
        text = input("[OPT] attribute {:d}, value: ".format(len(deviceAttributeValueList)+1))
        if text == "":
            break
        deviceAttributeValueList.append(text)
    deviceAttributeList = list(zip(deviceAttributeKeyList, deviceAttributeValueList))

    devhash = registerNewDevice(conffile, deviceName, deviceManuf, deviceType, deviceDescription, deviceAttributeList)
    if devhash == "":
        print()
        print("Failed to create new device!")
        quit()

    print()
    print("The device id for ", deviceName, " is: ", devhash, sep="")
    print()

    filename = input("Store to a file? Filename: ")
    if filename != "":
        with open(filename, "a") as file:
            file.write(deviceName + ": " + devhash + "\n")
