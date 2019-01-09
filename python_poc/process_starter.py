# -*- coding: utf-8 -*-
"""This module contains handles the starting/restaring of the Procem modules."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import adapters.common_utils as common_utils
import time
import subprocess
import sys


def get_screen_pids(screen_name, timeout=None):
    """Returns the screen pids corresponding to the given screen name."""
    try:
        command = ["screen", "-list"]
        screen_list = subprocess.run(command, timeout=timeout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if screen_list.returncode != 0:
            print(common_utils.getTimeString(), "get_screen_pid:", screen_list.stderr.decode("utf-8"))
            return []

        result_lines = screen_list.stdout.decode("utf-8").splitlines()
        screen_pids = []
        for line in result_lines:
            for word in line.strip().split("\t"):
                if "." + screen_name == word[-(len(screen_name) + 1):]:
                    screen_pids.append(word)

        return screen_pids

    except Exception as error:
        print(common_utils.getTimeString(), "get_screen_pid:", error)
        return []


def screen_window_exists(screen_name, window_name, timeout=None):
    """Returns True if there exists a window named window_name in a screen named screen_name."""
    if len(screen_name) == 0 or len(window_name) == 0:
        return False

    screen_pids = get_screen_pids(screen_name, timeout)
    if len(screen_pids) == 0:
        return False

    for screen_pid in screen_pids:
        try:
            command = ["screen", "-S",  screen_pid, "-Q", "windows"]
            window_list = subprocess.run(command, timeout=timeout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if window_list.returncode != 0:
                print(common_utils.getTimeString(), "screen_window_exists:", window_list.stderr.decode("utf-8"))

            for word in window_list.stdout.decode("utf-8").split():
                if window_name == word.strip():
                    return True

        except Exception as error:
            print(common_utils.getTimeString(), "screen_window_exists:", error)

    return False


def start_component(component_info, timeout=None):
    """Starts the given component if it is not already active."""
    active = component_info.get("active", False)
    if not active:
        return

    screen_name = component_info.get("screen_name", "")
    window_name = component_info.get("window_name", "")
    working_directory = component_info.get("working_directory", ".")
    start_command = component_info.get("start_command", "")
    if timeout is not None:
        timeout_command = ["timeout", str(timeout)]
    else:
        timeout_command = []

    if (screen_name == "" or window_name == "" or start_command == "" or
            screen_window_exists(screen_name, window_name, timeout)):
        return

    screen_pids = get_screen_pids(screen_name, timeout)
    if len(screen_pids) == 0:
        # the screen doesn't exist, so create a new one
        screen_command = "screen -d -m -L -S " + screen_name + " -t " + window_name
    else:
        if len(screen_pids) > 1:
            # use the first screen in the list
            screen_name = screen_pids[0]
        # add a new window to the existing screen
        screen_command = "screen -S " + screen_name + " -X screen -t " + window_name

    try:
        pipe = subprocess.Popen(" ".join(timeout_command + [screen_command, start_command]),
                                cwd=working_directory, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(common_utils.getTimeString(), "Started component:", (screen_name, window_name))

    except Exception as error:
        print(common_utils.getTimeString(), "Error while starting:", (screen_name, window_name), error)


def component_starter(components, wait_time):
    """Checks periodically the given components and restarts them if necessary."""
    while True:
        for component in components:
            start_component(component, wait_time / 100)
            time.sleep(5.0)

        print(common_utils.getTimeString(), "All components checked.")
        time.sleep(wait_time)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        config_file = sys.argv[1]
    else:
        print("Start this program with 'python3", sys.argv[0], "<configuration file>' command")
        quit()

    print(common_utils.getTimeString(), "Reading configuration parameters from", config_file)
    config = common_utils.readConfig(config_file)
    components = config["components"]
    wait_time = config["wait_time"]

    component_starter(components, wait_time)
