# -*- coding: utf-8 -*-

"""Data filter for ProCem/PooCemPlus data."""

import datetime
import glob
import os
import pathlib
import shutil
import sys
from typing import cast, List, Optional, Set, Union

import py7zr


class DataFilter:
    """
    Class for producing filtered CSV data out of the archived raw data collected in the ProCem and ProCemPlus projects.
    """
    ARCHIVE_EXTENSION = ".7z"
    RAW_DATA_EXTENSION = ".csv"
    FILENAME_IDENTIFIER_SEPARATOR = "_"

    def __init__(self, source_folder: pathlib.Path,
                 target_folder: pathlib.Path,
                 target_file_identifier: str,
                 measurement_ids: Set[int],
                 source_delimiter: str, target_delimiter: str,
                 overwrite: bool):
        self.source_folder = source_folder
        self.target_folder = target_folder
        self.target_file_identifier = target_file_identifier
        self.measurement_ids = measurement_ids
        self.source_delimiter = source_delimiter
        self.target_delimiter = target_delimiter
        self.overwrite = overwrite

    def filter_single_day(self, considered_date: datetime.date):
        """
        Writes filtered raw data files for a single day.
        - Copies the archived (.7z) and non-archived (.csv) files from the source folder to the target folder.
        - Extracts all the files from the copied archived files to the target folder.
        - Deletes the copied archived files from the target folder.
        - Goes through each of the copied files from the target folder and writes the filtered data files.
        - Deletes all the copied or extracted files from the target folder.
        """
        # find all the files from the source folder that start with the given date
        source_file_pattern = self.source_folder / (considered_date.isoformat() + "*")
        source_files = glob.glob(str(source_file_pattern), recursive=False)
        for source_file in source_files:
            source_file_path = pathlib.Path(source_file)

            if source_file_path.suffix not in (DataFilter.RAW_DATA_EXTENSION, DataFilter.ARCHIVE_EXTENSION):
                print("Ignoring file: '{}'".format(source_file), flush=True)
                continue

            # copy the file to the target folder
            source_copy_file_path = self.target_folder / source_file_path.name
            source_copy_check = self.copy_file(source_file_path, source_copy_file_path)
            if source_copy_check != source_copy_file_path:
                raise OSError(
                    "Filename '{}' does not match the expected: {}".format(source_copy_check, source_copy_file_path)
                )

            if source_file_path.suffix == DataFilter.ARCHIVE_EXTENSION:
                # found .7z file in the source folder
                extracted_files = self.extract_7zip_archive(source_copy_file_path)
                DataFilter.delete_file(source_copy_file_path)
            else:
                # found .csv file in the source folder
                extracted_files = [pathlib.Path(source_file_path.name)]

            # go through each of the extracted files and write the filtered data file
            for extracted_file in extracted_files:
                filtered_file_path = self.target_folder / extracted_file.with_name(
                    DataFilter.FILENAME_IDENTIFIER_SEPARATOR.join(
                        [
                            extracted_file.with_suffix("").name,
                            self.target_file_identifier
                        ]
                    )
                ).with_suffix(extracted_file.suffix)

                filtered_rows = self.filter_data(
                    source_file_path=self.target_folder / extracted_file,
                    target_file_path=filtered_file_path
                )
                DataFilter.delete_file(self.target_folder / extracted_file)

                if filtered_rows > 0:
                    print("Created filtered file '{}' with {} rows".format(filtered_file_path, filtered_rows),
                          flush=True)
                else:
                    print("No data for file '{}'".format(filtered_file_path), flush=True)
                print(flush=True)

    def filter_date_range(self, start_date: datetime.date, end_date: datetime.date):
        """
        Writes filtered raw data files for for the given date range.
        """
        if not self.target_folder.exists():
            print("Target folder '{}' does not exist".format(self.target_folder), flush=True)
            return
        if not self.target_folder.is_dir():
            print("Target folder '{}' is not a folder".format(self.target_folder), flush=True)
            return

        while start_date <= end_date:
            try:
                print("Started handling date: {}".format(start_date), flush=True)
                self.filter_single_day(start_date)
            except OSError as os_error:
                print("Encountered {}: {}\n".format(type(os_error).__name__, os_error), flush=True)
            start_date += datetime.timedelta(days=1)

    def copy_file(self, source_file: pathlib.Path, target_file: pathlib.Path) -> pathlib.Path:
        """
        Copies the contents from the source file to the target file.
        Returns the filename of the copied file.
        - source_file (Path): must correspond to a filename that the caller has read permission
        - target_file (Path): must correspond to a filename that the caller has write permission
        """
        if not source_file.exists():
            raise FileNotFoundError("path '{}' does not exist".format(source_file))
        if not source_file.is_file():
            raise FileNotFoundError("file '{}' is not a file".format(source_file))
        if not self.overwrite and target_file.exists():
            raise FileExistsError("file '{}' already exists".format(target_file))
        if target_file.is_dir():
            raise IsADirectoryError("'{}' is not a filename".format(target_file))

        print("Copying file '{}' to '{}'".format(source_file, target_file), flush=True)
        return pathlib.Path(
            shutil.copyfile(
                src=source_file,
                dst=str(target_file),
                follow_symlinks=True
            )
        )

    def extract_7zip_archive(self, archive_filename: pathlib.Path) -> List[pathlib.Path]:
        """
        Extracts the files from the given archive to the given target directory set in the object.
        Returns the list of filenames that were extracted.
        - archive_filename (Path): must correspond to a 7-Zip archive that the caller has read permission
        """
        print("Extracting file '{}'".format(archive_filename), flush=True)
        with py7zr.SevenZipFile(archive_filename, mode="r") as zip_file:
            zip_file.extractall(path=self.target_folder)
            return [
                pathlib.Path(filename)
                for filename in zip_file.getnames()
            ]

    def filter_data(self, source_file_path: pathlib.Path, target_file_path: pathlib.Path) -> int:
        """Loads the wanted measurements from the locally stored data and writes them to a new file."""
        print("Filtering file '{}' with id list {} to file '{}'".format(
            source_file_path, self.measurement_ids, target_file_path), flush=True)
        items = [""]
        target_row_count = 0

        if not self.overwrite and target_file_path.exists():
            raise FileExistsError("file '{}' already exists".format(target_file_path))

        try:
            with open(source_file_path, "r") as source_file, open(target_file_path, "w") as target_file:
                for source_row in source_file:
                    try:
                        items = source_row.strip().split(self.source_delimiter)
                        # ignore empty rows in the source file
                        if items:
                            measurement_id = int(items[0])
                            if measurement_id in self.measurement_ids:
                                target_row = self.target_delimiter.join(items) + "\n"
                                target_file.write(target_row)
                                target_row_count += 1
                    except ValueError:
                        print("Invalid measurement id in source file: '{}'".format(items[0]), flush=True)

            # If there were no filtered data for the target file, try to delete the created empty target file
            if target_row_count == 0 and target_file_path.exists() and target_file_path.is_file():
                DataFilter.delete_file(target_file_path)

        except OSError as file_error:
            print("Encountered {}: {}\n".format(type(file_error).__name__, file_error), flush=True)

        return target_row_count

    @staticmethod
    def delete_file(filename: pathlib.Path) -> None:
        """
        Deletes the file corresponding to the given filename.
        - filename (Path): must correspond to a filename that the caller has write permission
        """
        print("Deleting file '{}'".format(filename), flush=True)
        os.remove(filename)


def data_filter(source_folder: Union[str, pathlib.Path],
                target_folder: Union[str, pathlib.Path],
                start_date: Union[str, datetime.date],
                end_date: Union[str, datetime.date],
                measurement_ids: Union[int, str, List[int], List[str], List[Union[int, str]]],
                target_file_identifier: Optional[str] = None,
                source_delimiter: Optional[str] = None,
                target_delimiter: Optional[str] = None,
                overwrite: Optional[Union[bool, str]] = None):
    """
    Writes filtered data files from the archived raw data from the ProCem and ProCemPlus projects.
    Uses the given date range to choose which files to filter.

    - source_folder: the path to the source folder containing the raw data (read access is required)
    - target_folder: the path to the target folder where the filtered files are created (write access is required)
    - start_data: the start date from which to start the filtering process in ISO 8601 format: YYYY-MM-DD
    - end_data: the end date to which to end the filtering process in ISO 8601 format: YYYY-MM-DD
    - measurement_ids: a list of measurement ids that will be included in the filtered files
    - target_file_identifier: a string which will be added to the created filtered files,
                              if the original filename is "2020-01-01_procem.csv", then the filtered file name will be
                              "2020-01-01_procem_<target_file_identifier>.csv", the default value is "filtered"
    - source_delimiter: the column separator used in the source files, the default value is "\t"
    - target_delimiter: the column separator used in the filtered files,
                        the default value is the same as in the source files
    - overwrite: boolean value telling whether overwriting existing files is allowed,
                 the overwriting flag is only checked when copying files, not when extracting files from an archive
                 the default value is True
    """
    if isinstance(measurement_ids, int):
        measurement_ids = [measurement_ids]
    elif isinstance(measurement_ids, str):
        measurement_ids = measurement_ids.split(",")
    elif not isinstance(measurement_ids, (list, tuple, set)):
        measurement_ids = list(measurement_ids)
    considered_ids = {
        int(measurement_id)
        for measurement_id in measurement_ids
    }

    if not considered_ids:
        print("The measurement id list cannot be empty", flush=True)
        return

    if target_file_identifier is None:
        target_file_identifier = "filtered"

    if source_delimiter is None:
        source_delimiter = "\t"
    # Use the source file delimiter as the default target file delimiter
    if target_delimiter is None:
        target_delimiter = source_delimiter

    source_folder = pathlib.Path(source_folder)
    target_folder = pathlib.Path(target_folder)
    if target_folder == source_folder:
        print("The target folder must be different than the source folder", flush=True)
        return

    if overwrite is None:
        overwrite = True
    elif isinstance(overwrite, str):
        overwrite = overwrite.lower() == "true"
    elif not isinstance(overwrite, bool):
        overwrite = bool(overwrite)

    data_filter_object = DataFilter(
        source_folder=source_folder,
        target_folder=target_folder,
        target_file_identifier=target_file_identifier,
        measurement_ids=considered_ids,
        source_delimiter=source_delimiter,
        target_delimiter=target_delimiter,
        overwrite=overwrite
    )

    try:
        if not isinstance(start_date, datetime.date):
            start_date = datetime.date.fromisoformat(start_date)
        if not isinstance(end_date, datetime.date):
            end_date = datetime.date.fromisoformat(end_date)
    except (TypeError, ValueError) as date_error:
        print("The dates must be given in ISO 8601 format: {}".format(date_error), flush=True)
        return

    if end_date < start_date:
        print("The end date, {}, cannot be before the start date, {}".format(end_date, start_date), flush=True)
        return

    data_filter_object.filter_date_range(start_date, end_date)


if __name__ == "__main__":
    # Command line interface for the data filter.
    if len(sys.argv) < 6:
        print("Too few command line arguments.", flush=True)
        print("Usage: python {} <source_folder> <target_folder> <start_date> <end_date> ".format(sys.argv[0]) +
              "<measurement_ids> [<target_file_identifier>] [<target_delimiter>] [<source_delimiter>] [<overwrite>]",
              flush=True)
        sys.exit()

    arguments = [
        sys.argv[index] if index < len(sys.argv) else None
        for index in range(1, 10)
    ]

    data_filter(
        source_folder=cast(str, arguments[0]),
        target_folder=cast(str, arguments[1]),
        start_date=cast(str, arguments[2]),
        end_date=cast(str, arguments[3]),
        measurement_ids=cast(str, arguments[4]),
        target_file_identifier=arguments[5],
        target_delimiter=arguments[6],
        source_delimiter=arguments[7],
        overwrite=arguments[8]
    )
