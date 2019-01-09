# -*- coding: utf-8 -*-
"""Script for adding a license to all source code files."""

# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta

import fileinput
import pathlib
import sys

default_code_language = "Python"


def default_python_settings():
    """Returns the default license addition settings for Python source code files.
       By default the license will be added after the beginning comments, since in Python the encoding
       information can only be given as comments in the first or the second line. """
    return {
        "file_extension": [
            ".py"
        ],
        "recursive": True,
        "skip_start_comments": False,
        "single_line_comment": "#",
        "multi_line_comment_begin": [
            '"""',
            "'''"
        ],
        "multi_line_comment_end": [
            '"""',
            "'''"
        ],
        "empty_lines_before_license": 1,
        "empty_lines_after_license": 0
    }


def default_javascript_settings():
    """Returns the default license addition settings for JavaScript source code files.
       By default the license will be added after the beginning comments. """
    return {
        "file_extension": [
            ".js"
        ],
        "recursive": True,
        "skip_start_comments": False,
        "single_line_comment": "//",
        "multi_line_comment_begin": "/*",
        "multi_line_comment_end": "*/",
        "empty_lines_before_license": 1,
        "empty_lines_after_license": 1
    }


def default_c_settings():
    """Returns the default license addition settings for JavaScript source code files.
       By default the license will be added after the beginning comments. """
    return {
        "file_extension": [
            ".c",
            ".h"
        ],
        "recursive": True,
        "skip_start_comments": False,
        "single_line_comment": "//",
        "multi_line_comment_begin": "/*",
        "multi_line_comment_end": "*/",
        "empty_lines_before_license": 0,
        "empty_lines_after_license": 1
    }


def default_cplusplus_settings():
    """Returns the default license addition settings for JavaScript source code files.
       By default the license will be added after the beginning comments. """
    return {
        "file_extension": [
            ".cc",
            ".cpp",
            ".h",
            ".hh"
        ],
        "recursive": True,
        "skip_start_comments": False,
        "single_line_comment": "//",
        "multi_line_comment_begin": "/*",
        "multi_line_comment_end": "*/",
        "empty_lines_before_license": 1,
        "empty_lines_after_license": 1
    }


def default_text_settings():
    """Returns the default license addition settings for text files."""
    return {
        "file_extension": [
            ".txt",
            ".text"
        ],
        "recursive": True,
        "skip_start_comments": True,
        "single_line_comment": "##",
        "empty_lines_before_license": 0,
        "empty_lines_after_license": 1
    }


def default_settings(code_language=None, verbose=None):
    """Returns the default license addition settings."""
    if code_language is None:
        code_language = default_code_language
    if verbose is None:
        verbose = False

    if code_language.lower() == "python":
        return default_python_settings()
    elif code_language.lower() == "javascript":
        return default_javascript_settings()
    elif code_language.lower() == "c":
        return default_c_settings()
    elif code_language.lower() == "c++":
        return default_cplusplus_settings()
    elif code_language.lower() == "text":
        return default_text_settings()
    else:
        if verbose:
            print("Unknown source code language:", code_language)
            print("Using the default settings (", default_code_language, ").", sep="")
        return default_settings()


def add_license_to_file(filename, license_text, settings):
    """Adds the given license to the given file using the given settings. Only a simple parser for recognizing comment
       lines (for example starting a new multiline comment in the same line as the previous ended is not recognized)."""
    full_license_text = "\n" * settings["empty_lines_before_license"]
    full_license_text += license_text
    full_license_text += "\n" * settings["empty_lines_after_license"]

    skip_start = settings["skip_start_comments"]
    single_comment = settings["single_line_comment"]
    multi_comment_begin = settings.get("multi_line_comment_begin", None)
    multi_comment_end = settings.get("multi_line_comment_end", None)
    if multi_comment_begin is None or multi_comment_end is None:
        use_multi_comment = False
    else:
        use_multi_comment = True
        if not isinstance(multi_comment_begin, list):
            multi_comment_begin = [multi_comment_begin]
        if not isinstance(multi_comment_end, list):
            multi_comment_end = [multi_comment_end]

    license_written = False
    comment_open = -1
    for line in fileinput.FileInput(files=filename, inplace=1):
        wait_for_next_line = False
        if not license_written:
            stripped_line = line.strip()
            if comment_open >= 0:
                if use_multi_comment and multi_comment_end[comment_open] in stripped_line:
                    comment_open = -1
            else:
                if use_multi_comment:
                    for index, comment_begin in enumerate(multi_comment_begin):
                        if stripped_line[:len(comment_begin)] == comment_begin:
                            stripped_line = stripped_line[stripped_line.index(comment_begin) + len(comment_begin):]
                            if multi_comment_end[index] not in stripped_line:
                                comment_open = index
                                break
                            else:
                                wait_for_next_line = True

                if (comment_open < 0 and not wait_for_next_line and
                        (skip_start or not stripped_line[:len(single_comment)] == single_comment)):
                    print(full_license_text, end="")
                    license_written = True

        print(line, end="")


def add_license_to_dir(root_directory, license_text=None, license_file=None, code_language=None, verbose=None):
    """Adds the given license to all source code files in the given directory.
       The license can be given as a string by parameter license_text or as a file name by parameter license_file.
       If both are present license_text is used.
    """
    if license_text is None and license_file is None:
        print("No license given!")
        return

    settings = default_settings(code_language=code_language, verbose=verbose)
    single_comment = settings["single_line_comment"]

    if license_text is None:
        license_text = ""
        with open(license_file, mode="r", encoding="utf-8") as open_file:
            for line in open_file:
                if len(line) < 2:
                    license_text += "".join([single_comment, line])
                else:
                    license_text += " ".join([single_comment, line])

    root_path = pathlib.Path(root_directory)
    if not root_path.is_dir():
        print(root_directory, "is NOT a directory!")
        return

    for file_extension in settings["file_extension"]:
        source_file_pattern = "*" + file_extension
        source_files = root_path.glob(source_file_pattern)
        for source_file in source_files:
            print("Adding license to", source_file)
            add_license_to_file(str(source_file), license_text, settings)

    if settings["recursive"]:
        all_files = root_path.glob("*")
        for filename in all_files:
            file_path = pathlib.Path(filename)
            if file_path.is_dir():
                add_license_to_dir(filename, license_text=license_text, code_language=code_language, verbose=False)


if __name__ == "__main__":
    if len(sys.argv) == 3:
        directory = sys.argv[1]
        license_file = sys.argv[2]
    else:
        print("Start this program with 'python3", sys.argv[0], "<root directory> <license file>' command")
        quit()

    code_languages = [
        "Python",
        "JavaScript",
        "C",
        "C++",
        "Text"
    ]
    for language in code_languages:
        add_license_to_dir(directory, license_file=license_file, code_language=language)
