# -*- coding: utf-8 -*-
"""This module contains helper functions to parse the messages send by MXElectrix measurement device."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta


def getLineType(messageLine):
    """Returns a line type for the message line. The line type is either "header", "format", "data"
       or empty string if the line doesn't have at least 2 components separated by tabulator."""
    words = messageLine.strip().split('\t')

    if len(words) > 1:
        # header lines start as "distributor_id=something"
        check = "distributor_id"
        if words[0][:len(check)] == check:
            return "header"

        # apparently all message format lines start with date? For sure?!
        if words[0] == "date":
            return "format"

        # anything else should be treated as data?
        return "data"

    else:
        # Return empty instead None to make things easier (one cannot compare a string to None)
        return ""


def getHeaderType(messageLine):
    """Returns the header type for the message line."""
    return parseSettingValue(messageLine, "type")


def parseModuleId(messageLine):
    """Parses and returns the module_id from the message line."""
    return parseSettingValue(messageLine, "module_id")


def parseSettingValue(messageLine, setting):
    """Parses and returns a setting value from the message line.
       The setting value is identified by setting="value"."""
    words = messageLine.strip().split('\t')

    settingString = setting + '="'
    for word in words:
        mark = word.find(settingString)
        if mark >= 0:
            value = word[len(settingString):].rstrip('"')
            return value

    return None


def parseUnit(formatString, undefined="NaN"):
    """Returns the unit of data item from MXElectric data message enclosed in [].
       Returns empty string as parse error. If the unit is not defined, string parameter
       undefined can be used to set it (defaults to NaN)."""
    opening = formatString.find('[')
    closing = formatString.find(']')
    if closing < 0 or opening < 0:
        # did not find both brackets, returning empty string
        return ""
    elif closing - opening <= 1:
        # brackets in the wrong order or no unit defined, returning undefined
        return undefined
    else:
        return formatString[opening+1:closing]


def parseValue(formatString):
    """Returns the value type of data item from MXElectrix data message.
       The value type is taken to be everything before opening bracket [."""
    sep = formatString.find("[")
    if sep < 0:
        return ""
    else:
        return formatString[:sep]


def getStreamFormat(longFormatString):
    """Parses a format string to a list of (value, unit) tuples."""
    formatStringList = longFormatString.strip().split('\t')
    # should we replace all possible danger chars such as "\/?;:{}"?

    formatList = []
    for formatString in formatStringList:
        value = parseValue(formatString)
        unit = parseUnit(formatString)
        formatList.append((value, unit))
    return formatList


def parseMeasurement(messageLine, streamFormat):
    """Parses get_stream measurement line into list of tuples.
       Must contain same amount of items that the formatting has provided"""
    meas = messageLine.strip().split('\t')
    if len(meas) == len(streamFormat):
        # zip() is a neat python built in function that combines two lists to one list of tuples when typed to list()
        return list(zip(streamFormat, meas))
    else:
        return None
