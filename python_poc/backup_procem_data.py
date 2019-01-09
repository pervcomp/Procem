# -*- coding: utf-8 -*-
"""This module handles the compression and the backing up of the locally stored Procem data."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import adapters.common_utils as common_utils
import datetime
import glob
import os
import subprocess
import threading
import time
import sys

# the default configurations filename
CONF_FILE = "backup_procem_data.json"


def long_sleep(seconds):
    """Sleeps the given number of seconds."""
    max_interval = 3600.0  # sleep in one hour intervals
    start = time.time()
    while time.time() - start < seconds:
        interval = min(max_interval, seconds - (time.time() - start))
        time.sleep(interval)


def replace_extension(filename, new_extension, add_to_end=False):
    """Replaces the extension in the filename with the new_extension."""
    dot = filename.rfind(".")
    if dot < 0 or filename[dot + 1:] == new_extension or add_to_end:
        filename_base = filename
    else:
        filename_base = filename[:dot]
    return filename_base + "." + new_extension


def combine_counter_values(filename, datacounter_delimiter):
    """Combines the data counter values so that each id only has one entry in the file."""
    multiple_values = False
    data = {}
    with open(filename, "r") as file:
        for row in file:
            items = row.split(datacounter_delimiter)
            rtl_id = int(items[0])
            value = int(items[1])
            if rtl_id in data:
                data[rtl_id] += value
                multiple_values = True
            else:
                data[rtl_id] = value

    if multiple_values:
        with open(filename, "w") as file:
            for rtl_id, value in sorted(data.items()):
                file.write(datacounter_delimiter.join([str(rtl_id), str(value)]) + "\n")


def compress_data(filename, config=None):
    """Compresses the given file. If the compression is successful, removes the original file."""
    if config is None:
        compression_command = "7z a"
        compressed_file_extension = "7z"
        successful_compression_message = "Ok"
    else:
        compression_command = config["command"]
        compressed_file_extension = config["extension"]
        successful_compression_message = config["success_message"]

    compressed_file = replace_extension(filename, compressed_file_extension)
    print(common_utils.getTimeString(), "BACKUP: Compressing", filename)

    try:
        command = compression_command.split(" ") + [compressed_file, filename]
        compression = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # if the compression was successful, remove the original file
        if compression.returncode == 0:
            print_out = compression.stdout.decode("utf-8")
            if successful_compression_message in print_out:
                print(common_utils.getTimeString(), "BACKUP: Compression of", filename, "successful.")
                remove_data(filename)

    except Exception as error:
        print(common_utils.getTimeString(), "BACKUP:", error)


def change_file_permission(filename, permissions, remote_server=None, verbose=True):
    """Changes the given file to the given permissions."""
    if permissions is None:
        return True
    try:
        command = ["chmod", permissions, filename]
        if remote_server is not None:
            command = ["ssh", remote_server] + command
        check = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return check.returncode == 0 and check.stdout.decode("utf-8") == "" and check.stderr.decode("utf-8") == ""
    except Exception as error:
        if verbose:
            print(common_utils.getTimeString(), "CHMOD:", error)
        return False


def get_md5sum(filename, remote_server=None):
    """Returns the md5 checksum of the given file."""
    try:
        command = ["md5sum", filename]
        if remote_server is not None:
            command = ["ssh", remote_server] + command

        check = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if check.returncode == 0 and check.stderr.decode("utf-8") == "":
            # strip the file name out of the print out
            return check.stdout.decode("utf-8").strip().split(" ")[0]
        else:
            return None

    except Exception as error:
        print(common_utils.getTimeString(), "MD5SUM:", error)
        return None


def backup_data(source_file, target_directory, file_permissions=None, remote_server=None):
    """Backs up the given file. If file_permission is not None, changes the target file permissions
       to them after the copying is done. Returns True if the backup was successful."""
    try:
        target_file = os.path.join(target_directory, os.path.basename(source_file))

        # first, check whether the file already exists in the backup directory
        if remote_server is None:
            check = subprocess.run(["cmp", source_file, target_file],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if check.returncode == 0 and check.stdout.decode("utf-8") == "" and check.stderr.decode("utf-8") == "":
                return True
        else:
            # With remote files only check that the md5 checksums match
            target_md5 = get_md5sum(target_file, remote_server)
            if target_md5 is not None:
                source_md5 = get_md5sum(source_file)
                if source_md5 is not None and source_md5 == target_md5:
                    return True

        if remote_server is not None:
            # print the backup message only when copying to remote server
            print(common_utils.getTimeString(), "BACKUP: Backing up", source_file)

        if remote_server is None:
            command = ["cp", source_file, target_directory]
        else:
            command = ["scp", source_file, ":".join([remote_server, target_directory])]
        change_file_permission(target_file, "u+w", remote_server, False)
        copying = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if copying.returncode != 0:
            print(common_utils.getTimeString(), "BACKUP:", source_file, copying.stderr.decode("utf-8"))
            return False
        else:
            return change_file_permission(target_file, file_permissions, remote_server)

    except Exception as error:
        print(common_utils.getTimeString(), "BACKUP:", error)
        return False


def remove_data(filename):
    """Removes the given file."""
    print(common_utils.getTimeString(), "BACKUP: Removing", filename)
    try:
        removal = subprocess.run(["rm", filename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if removal.returncode != 0:
            print(common_utils.getTimeString(), "BACKUP: Removal of", filename, "was NOT successful.")

    except Exception as error:
        print(common_utils.getTimeString(), "BACKUP:", error)


def sleep_until_backup_time(backup_hour):
    """Sleeps until the next backup time."""
    current_time = datetime.datetime.now()
    backup_time_today = current_time.replace(hour=backup_hour, minute=0, second=0, microsecond=0)
    if current_time.hour < backup_hour:
        backup_time = backup_time_today
    else:
        backup_time = backup_time_today + datetime.timedelta(days=1)
    time_interval = backup_time - current_time

    # go to sleep until next backup time
    total_seconds = int(time_interval.total_seconds())
    hours = total_seconds // 3600
    minutes = total_seconds % 3600 // 60
    seconds = total_seconds % 60
    print(common_utils.getTimeString(),
          "Data backup program going to sleep for {:0>2d}:{:0>2d}:{:0>2d}".format(hours, minutes, seconds))

    long_sleep(time_interval.total_seconds())


def backup_worker(configuration):
    """Handles the periodic backing up of the data."""
    data_filename = configuration["data_filename"]
    datacounter_filename = configuration["datacounter_filename"]
    datacounter_delimiter = configuration["datacounter_delimiter"]
    local_file_permissions = configuration["local_file_permissions"]
    remote_file_permissions = configuration["remote_file_permissions"]

    local_data_directory = configuration["local_data_directory"]
    local_counter_directory = configuration["local_counter_directory"]
    remote_backup_server = configuration["remote_backup_server"]
    remote_data_directory = configuration["remote_data_directory"]
    remote_counter_directory = configuration["remote_counter_directory"]

    compresion_command = configuration["compression_command"]
    compressed_file_extension = configuration["compressed_file_extension"]
    compression_success_message = configuration["compression_success_message"]

    data_backup_hour = configuration["data_backup_hour"]
    file_keep_days_cwd = configuration["file_keep_days_cwd"]
    file_keep_days_local_backup = configuration["file_keep_days_local_backup"]

    while True:
        # set the base name for compressed data file names
        compressed_data_basename = replace_extension(data_filename, compressed_file_extension)

        # construct today's file names
        current_time = datetime.datetime.now()
        current_files = [
            data_filename.format(year=current_time.year, month=current_time.month, day=current_time.day),
            compressed_data_basename.format(year=current_time.year, month=current_time.month, day=current_time.day),
            datacounter_filename.format(year=current_time.year, month=current_time.month, day=current_time.day)
        ]

        # construct file names for days for which the data is kept in the current working directory
        cwd_keep_files = []
        for day in range(1, file_keep_days_cwd + 1, 1):
            dt = current_time - datetime.timedelta(days=day)
            cwd_keep_files.append(data_filename.format(year=dt.year, month=dt.month, day=dt.day))
            cwd_keep_files.append(compressed_data_basename.format(year=dt.year, month=dt.month, day=dt.day))
            cwd_keep_files.append(datacounter_filename.format(year=dt.year, month=dt.month, day=dt.day))
        cwd_keep_files += current_files

        # construct file names for days for which the data is kept in the local backup directory
        local_keep_files = []
        for day in range(file_keep_days_cwd + 1, file_keep_days_local_backup + 1, 1):
            dt = current_time - datetime.timedelta(days=day)
            local_keep_files.append(data_filename.format(year=dt.year, month=dt.month, day=dt.day))
            local_keep_files.append(compressed_data_basename.format(year=dt.year, month=dt.month, day=dt.day))
            local_keep_files.append(datacounter_filename.format(year=dt.year, month=dt.month, day=dt.day))
        local_keep_files += cwd_keep_files

        # gather the existing data file names in the current working directory
        data_files = glob.glob(data_filename.format(year="????", month="??", day="??"))

        # compress the data files
        compress_config = {
            "command": compresion_command,
            "extension": compressed_file_extension,
            "success_message": compression_success_message
        }
        for file in data_files:
            if file not in current_files:
                compress_data(file, config=compress_config)

        # gather the existing data counter file names and combine values within each file
        datacounter_files = glob.glob(datacounter_filename.format(year="????", month="??", day="??"))
        for file in datacounter_files:
            combine_counter_values(file, datacounter_delimiter)

        # gather the existing compressed data file names in the current working directory
        compressed_data_files = glob.glob(compressed_data_basename.format(year="????", month="??", day="??"))

        # backup the compressed data and the data counter files to the local backup directory
        local_failed_backups = []
        directories = (
            [local_data_directory] * len(compressed_data_files) +
            [local_counter_directory] * len(datacounter_files))
        for file, directory in zip(compressed_data_files + datacounter_files, directories):
            if file not in current_files:
                if not backup_data(file, directory, local_file_permissions):
                    local_failed_backups.append(file)

        # remove the old compressed data and data counter files the current working directory
        for file in compressed_data_files + datacounter_files:
            if file not in cwd_keep_files and file not in local_failed_backups:
                remove_data(file)

        # gather the existing compressed data file names in the local backup directory
        compressed_local_data_base = os.path.join(local_data_directory, compressed_data_basename)
        compressed_data_backup = glob.glob(compressed_local_data_base.format(year="????", month="??", day="??"))

        # gather the existing data counter file names in the local backup directory
        compressed_local_counter_base = os.path.join(local_counter_directory, datacounter_filename)
        datacounter_backup = glob.glob(compressed_local_counter_base.format(year="????", month="??", day="??"))

        # backup the compressed data and the data counter files from the local backup directory to the remote server
        remote_failed_backups = []
        remote_directories = (
            [remote_data_directory] * len(compressed_data_backup) +
            [remote_counter_directory] * len(datacounter_backup))
        for file, directory in zip(compressed_data_backup + datacounter_backup, remote_directories):
            if file not in current_files:
                if not backup_data(file, directory, remote_file_permissions, remote_backup_server):
                    remote_failed_backups.append(file)

        # remove the old compressed data and data counter files from the local backup directory
        for file in compressed_data_backup + datacounter_backup:
            if os.path.basename(file) not in local_keep_files and file not in remote_failed_backups:
                remove_data(file)

        # the backup is done once a day at the start of the hour BACKUP_HOUR
        sleep_until_backup_time(data_backup_hour)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        PROCEM_CONF_FILE = sys.argv[1]
    elif len(sys.argv) != 1:
        print("Start this program with 'python3", sys.argv[0], "config_file.json' command")
        print("or use 'python3 ", sys.argv[0], "' to use the default configuration filename: ", CONF_FILE, sep="")
        quit()

    # read configuration file
    # TODO: add sanity checks for the parameters
    jd = common_utils.readConfig(CONF_FILE)
    conf = {}
    date_format = jd.get("date_format", "{year:0>4}-{month:0>2}-{day:0>2}")
    conf["data_filename"] = date_format + jd.get("data_filename_end", "_procem.csv")
    conf["datacounter_filename"] = date_format + jd.get("counter_filename_end", "_data_counter.csv")
    conf["datacounter_delimiter"] = jd.get("counter_delimiter", "\t")
    conf["local_file_permissions"] = jd.get("local_file_permissions", None)
    conf["remote_file_permissions"] = jd.get("remote_file_permissions", None)

    conf["local_data_directory"] = jd["local_data_directory"]
    conf["local_counter_directory"] = jd["local_counter_directory"]
    conf["remote_backup_server"] = jd["remote_backup_server"]
    conf["remote_data_directory"] = jd["remote_data_directory"]
    conf["remote_counter_directory"] = jd["remote_counter_directory"]

    conf["compression_command"] = jd.get("compression_command", "7z a")
    conf["compressed_file_extension"] = jd.get("compressed_file_extension", "7z")
    conf["compression_success_message"] = jd.get("compression_success_message", "")

    # the hour on which the data backup is started each day
    conf["data_backup_hour"] = jd.get("backup_hour", 2)
    # number of days to keep the files on the current working directory
    conf["file_keep_days_cwd"] = jd.get("backup_days_cwd", 3)
    # number of days to keep the files on the local backup directory
    conf["file_keep_days_local_backup"] = max(jd.get("backup_days_backup_dir", 7), conf["file_keep_days_cwd"])

    # start the data backup worker
    backup_thread = threading.Thread(target=backup_worker, daemon=True, args=(conf,))
    backup_thread.start()
    backup_thread.join()
