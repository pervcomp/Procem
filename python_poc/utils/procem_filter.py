# -*- coding: utf-8 -*-
"""Module for filtering out the wanted data from a Procem data file."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta


def filter_data(source_filename, target_filename, rtl_ids, delimiter="\t"):
    """Loads the wanted measurements from the locally stored data and writes them to a new file."""
    if type(rtl_ids) is not list:
        try:
            ids = list(rtl_ids)
        except TypeError:
            ids = [rtl_ids]
    else:
        ids = rtl_ids
    ids = [int(rtl_id) for rtl_id in ids]

    try:
        with open(source_filename, "r") as source_file, open(target_filename, "w") as target_file:
            for row in source_file:
                try:
                    items = row.strip().split(delimiter)
                    rtl_id = int(items[0])
                    if rtl_id in ids:
                        target_file.write(row)
                except ValueError:
                    pass

    except Exception as error:
        print("ERROR:", error)
