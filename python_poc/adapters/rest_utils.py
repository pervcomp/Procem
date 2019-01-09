# -*- coding: utf-8 -*-
"""This module has helper functions for doing a query from a REST API."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import datetime
import requests

STATUS_OK = 200


def getStringParams(param_str):
    """Determines which keys are required if param_str.format() function is used.
       Returns the required keys as a list.
    """
    params = []
    index = 0
    while True:
        index = param_str.find("{", index)
        if index >= 0:
            end_index = param_str.find("}", index)
            if end_index > index:
                double_colon_index = param_str.find(":", index)
                if index < double_colon_index < end_index:
                    params.append(param_str[index+1:double_colon_index])
            index += 1
        else:
            break

    return params


def getParameterValue(param, **kwargs):
    """Determines and returns the name and value of the given parameter."""
    # check the parameter name
    name = param.get("name", None)
    if name is None:
        return "", ""

    # get the value of the parameter
    value = kwargs.get(name, None)
    if value is None:
        value = param.get("default", None)
        if value is None:
            return "", ""

    # change the value to a string
    value_type = param.get("type", None)
    if value_type == "timestamp":
        dt = datetime.datetime.utcfromtimestamp(float(value))
        datetime_format = kwargs.get("config", {}).get("datetime_format", "")
        value = datetime.datetime.strftime(dt, datetime_format)
    elif value_type == "list":
        element_separator = param.get("element_separator", ",")
        value = [str(item) for item in value]
        value = element_separator.join(value)
    else:
        value = str(value)

    return name, value


def getAPIQuery(**kwargs):
    """Determines and returns the parameters for an API query corresponding the given function parameters.
       The return value is 3-tuple (method, address, headers) in which method is the query method (GET, POST, etc.),
       address is the HTTP-address and headers are the required header parameters for the query.
    """
    # get the configuration scheme for the query
    config = kwargs.get("config", {})

    # get the query method, host address and authentication information
    method = config.get("method", "")
    host = config.get("host", "")
    authentication = config.get("authentication", {})
    authentication_type = authentication.get("type", "none")

    # get the api key if it is required in the address
    current_params = {}
    headers = {}
    if authentication_type == "address":  # api-key is used in the address
        api_key = authentication.get("api_key", None)
        if api_key is not None:
            current_params["api_key"] = api_key
    elif authentication_type == "header":  # the authentication is done in the header
        param_list = authentication.get("params", [])
        for param in param_list:
            field = param.get("field", None)
            value = param.get("value", None)
            if field is not None and value is not None:
                headers[field] = value

    # check the used parameter values
    dynamic_params = config.get("dynamic_params", [])
    for param in dynamic_params:
        (name, value) = getParameterValue(param, **kwargs)
        # add nonempty parameters to the current parameters
        if name != "" and value != "":
            current_params[name] = value

    # check which of the query parameters can be used in this case
    query_params = config.get("query_params", {})
    used_query_params = {}
    for param, value in query_params.items():
        required_params = getStringParams(value)
        used_param = True
        for required_param in required_params:
            if required_param not in current_params:
                used_param = False
                break
        if used_param:
            used_query_params[param] = value

    # build the query parameters for the address
    query = ""
    for index, (param, value) in enumerate(used_query_params.items()):
        if index == 0:
            query += "?"
        else:
            query += "&"
        query += param + "=" + value

    # build the address using the current values of the parameters
    address = (host + query).format(**current_params)

    return method, address, headers


def runAPIQuery(**kwargs):
    """Runs an API query and returns the result."""
    (method, address, headers) = getAPIQuery(**kwargs)
    if method == "GET":
        return requests.get(address, headers=headers)
    elif method == "POST":
        return requests.post(address, headers=headers)
    else:
        return None
