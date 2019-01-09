# -*- coding: utf-8 -*-

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

from pymodbus.client.sync import ModbusTcpClient as ModbusClient
import sys
import struct

target_ip = "127.0.0.1"  # NOTE: insert target ip address
target_port = 0  # NOTE: insert target port number
source_ip = "127.0.0.1"  # NOTE: insert source ip address
source_port = 0  # NOTE: insert source port number

kwargs = {"host": target_ip, "port": target_port, "source_address": (source_ip, source_port)}
client = ModbusClient(**kwargs)
client.connect()


def read4floats(unitid, register):
    count = 8
    regs = []
    while len(regs) < 8:
        try:
            resp = client.read_holding_registers(register + len(regs), count, unit=unitid)
            regs += resp.registers
        except:
            print("Error when reading", count, "registers!")
            count = max(1, count // 2)

    values = [toFloat32(regs[i:i + 2]) for i in range(0, 8, 2)]
    return values


def read4ints(unitid, register):
    count = 4
    regs = []
    while len(regs) < 4:
        try:
            resp = client.read_holding_registers(register + len(regs), count, unit=unitid)
            regs += resp.registers
        except:
            print("Error when reading", count, "registers!")
            count = max(1, count // 2)

    values = [toInt16(regs[i:i + 1]) for i in range(0, 4, 1)]
    return values


def read3bigints(unitid, register):
    count = 12
    regs = []
    while len(regs) < 12:
        try:
            resp = client.read_holding_registers(register + len(regs), count, unit=unitid)
            regs += resp.registers
        except:
            print("Error when reading", count, "registers!")
            count = max(1, count // 2)

    values = [toInt64(regs[i:i + 4]) for i in range(0, 12, 4)]
    return values


def read_holding(unitid, register, count):
    try:
        resp = client.read_holding_registers(register, count, unit=unitid)
        try:
            return resp.registers
        except:
            print(resp)
            return []
    except Exception as error:
        print("ERROR:", error)
        return []


def read_input(unitid, register, count):
    try:
        resp = client.read_input_registers(register, count, unit=unitid)
        try:
            return resp.registers
        except Exception as error:
            print("ERROR:", error)
            return resp
    except Exception as error:
        print("ERROR:", error)
        return []


def write_register(unitid, register, value):
    resp = client.write_register(register, value, unit=unitid)
    return resp


def toFloat32(values, word_order="big", byte_order="little"):
    if word_order == "little":
        values = reversed(values)
    if byte_order == "little":
        endian = "<"
    else:
        endian = ">"
    return struct.unpack("f", struct.pack(endian + "2H", *values))[0]


def toInt16(values, word_order="big", byte_order="big", signed=False):
    if byte_order == "little":
        byte_endian = "<"
    else:
        byte_endian = ">"
    if word_order == "little":
        word_endian = "<"
    else:
        word_endian = ">"
    if signed:
        code = "h"
    else:
        code = "H"
    return struct.unpack(word_endian + code, struct.pack(byte_endian + "H", *values))[0]


def toInt32(values, word_order="big", byte_order="big", signed=False):
    if byte_order == "little":
        byte_endian = "<"
    else:
        byte_endian = ">"
    if word_order == "little":
        word_endian = "<"
    else:
        word_endian = ">"
    if signed:
        code = "i"
    else:
        code = "I"
    return struct.unpack(word_endian + code, struct.pack(byte_endian + "2H", *values))[0]


def toInt64(values, word_order="big", byte_order="big", signed=False):
    if byte_order == "little":
        byte_endian = "<"
    else:
        byte_endian = ">"
    if word_order == "little":
        word_endian = "<"
    else:
        word_endian = ">"
    if signed:
        code = "q"
    else:
        code = "Q"
    return struct.unpack(word_endian + code, struct.pack(byte_endian + "4H", *values))[0]


def toMod10(values, word_order="big"):
    if word_order == "little":
        values = reversed(values)
    value = 0
    for x in values:
        value *= 10000
        value += x
    return value
