# -*- coding: utf-8 -*-
"""This module contains the a helper function for parsing Modbus data."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

try:
    import adapters.common_utils as common_utils
except:
    # used when running the module directly
    import common_utils


def getProcemRTLpkt(register_values, register_type, timestamp):
    """Parses the register values and returns a Procem data packet."""
    path = register_type.path
    name = register_type.name
    unit = register_type.unit
    datatype = register_type.data_type
    rtl_id = register_type.rtl_id
    confidential = not register_type.ticket

    value = register_type.calculateValue(register_values)
    if value is None:
        return None

    # Create a new Procem RTL packet from the data
    new_pkt = common_utils.getProcemRTLpkt(
        name=name,
        path=path,
        value=value,
        timestamp=timestamp,
        unit=unit,
        datatype=datatype,
        variableNumber=rtl_id,
        confidential=confidential)
    return bytes(new_pkt, "utf-8")
