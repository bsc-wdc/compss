#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# -*- coding: utf-8 -*-

"""
This package contains the tools for automatic import discovery.

Summary of the Code:

- get_imports_from_file: Parses a Python file and extracts all the import
                         statements (e.g., import pandas or
                         from pandas import DataFrame).
- resolve_module_path: Resolves the file system path for a given module.
- is_standard_library: Determines if the given path belongs to the Python
                       standard library.
- get_imports: Iteratively collects all imported modules, considering
               sub-imports up to a maximum depth.
- print_import_tree: Nicely prints the nested structure of imports in a
                     readable format.
- main: Main entry point that initiates the import analysis on a given file.
- file_exists: A custom argparse type that checks whether the specified file
               exists.
- parse_arguments: Parses command-line arguments for file path, depth, and
                   debug options.

The script is designed to help analyze and visualize the import structure of a
Python file, iteratively exploring sub-imports while distinguishing between
built-in and non-built-in.
"""

import argparse
import ast
import importlib.util
import sys
import sysconfig
from pathlib import Path


def get_imports_from_file(file_path):
    """
    Parses a Python file and extracts all import statements (including from-imports).

    Args:
        file_path (str or Path): The path to the Python file to analyze.

    Returns:
        set: A set of fully qualified import names (e.g., 'pandas.core.frame').

    Raises:
        FileNotFoundError: If the file is not found.
        SyntaxError: If the file contains invalid Python syntax.
        UnicodeDecodeError: If there is a problem decoding the file.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            node = ast.parse(f.read(), filename=str(file_path))
    except (FileNotFoundError, SyntaxError, UnicodeDecodeError):
        return set()

    imports = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Import):
            for alias in n.names:
                imports.add(alias.name)
        if isinstance(n, ast.ImportFrom):
            if n.module:
                imports.add(n.module)
    return imports


def resolve_module_path(module_name):
    """
    Resolves the file system path of a module based on its name.

    Args:
        module_name (str): The name of the module to resolve (e.g., 'pandas').

    Returns:
        Path or None: Returns the file path of the module if found, otherwise
                      None.
    """
    try:
        spec = importlib.util.find_spec(module_name)
    except (ModuleNotFoundError, ValueError):
        return None
    if not spec or spec.origin in (None, "built-in", "frozen"):
        return None
    origin = spec.origin
    if origin.endswith((".pyc", ".pyo")):
        origin = origin[:-1]
    path = Path(origin)
    return path if path.exists() else None


def is_standard_library(path):
    """
    Determines if a given file path belongs to the Python standard library.

    Args:
        path (Path): The path to check.

    Returns:
        bool: True if the path is part of the standard library, False otherwise.
    """
    stdlib_dir = Path(sysconfig.get_paths()["stdlib"])
    return stdlib_dir in path.parents or path == stdlib_dir


def get_imports(
    file_path,
    max_depth=2,
    debug=False,
):
    """
    Recursively collects imported modules from a Python file, and submodules
    up to a maximum depth.

    Args:
        file_path (str or Path): The Python file to analyze for imports.
        max_depth (int, optional): The maximum recursion depth to follow
                                   sub-imports (default is 2).
        debug (bool, optional): Whether to output debug information
                                (default is False).

    Returns:
        tuple: A tuple containing:
            - result (dict): A dictionary representing the nested structure
                             of imports.
            - unique_imports (set): A set of unique imports found across
                                    all files.
    """
    visited = set()
    unique_imports = set()

    result = {}
    stack = [
        (file_path, None, 0)
    ]  # (current_file_path, current_prefix, current_depth)

    while stack:
        current_file, current_prefix, current_depth = stack.pop()

        if current_file in visited:
            continue
        visited.add(current_file)

        imports = get_imports_from_file(current_file)

        for imp in sorted(imports):
            if imp in visited:
                continue
            visited.add(imp)

            path = resolve_module_path(imp)
            if not path:
                if debug:
                    result[imp] = "(built-in or not found)"
                unique_imports.add(
                    f"{current_prefix}.{imp}" if current_prefix else imp
                )
                continue

            if is_standard_library(path):
                if debug:
                    result[imp] = "(standard library)"
                unique_imports.add(imp)
                continue

            if current_depth < max_depth:
                # Prepare the sub-imports for the next iteration
                stack.append(
                    (path, imp, current_depth + 1)
                )  # Push the next file to process
                if debug:
                    result[imp] = "(sub-imports to be explored)"
            else:
                if debug:
                    result[imp] = "(max depth reached)"
            unique_imports.add(imp)

    return result, unique_imports


def print_import_tree(tree, indent=0):
    """
    Prints the nested structure of imports in a readable format.

    Args:
        tree (dict): A dictionary representing the nested import tree.
        indent (int, optional): The indentation level for each line of output (default is 0).
    """
    for module, sub in tree.items():
        print("  " * indent + f"- {module}")
        if isinstance(sub, dict):
            print_import_tree(sub, indent + 1)
        elif isinstance(sub, str):
            print("  " * (indent + 1) + sub)


def main(file_path, max_depth=2, debug=False):
    """
    Main function to analyze imports in a given Python file and recursively
    explore sub-imports.

    Args:
        file_path (str or Path): The path to the Python file to analyze.
        max_depth (int, optional): Maximum depth for sub-import exploration
                                   (default is 2).
        debug (bool, optional): Whether to enable debug mode
                                (default is False).

    Returns:
        tuple: A tuple containing:
            - result_tree (dict): A dictionary representing the nested
                                  import tree.
            - result_unique_imports (set): A set of unique imports found across
                                           the file and sub-imports.
    """
    if debug:
        print(f"Analyzing imports in: {file_path}")
        print("(Recursing only into installed packages)\n")

    result_tree, result_unique_imports = get_imports(
        file_path, max_depth, debug
    )

    return result_tree, result_unique_imports


def file_exists(path_string: str) -> Path:
    """
    Custom argument type for argparse to check if the given file exists.

    Args:
        path_string (str): The path to check.

    Returns:
        Path: The Path object corresponding to the file.

    Raises:
        argparse.ArgumentTypeError: If the file does not exist.
    """
    path = Path(path_string)
    if not path.exists():
        raise argparse.ArgumentTypeError(f"The file '{path}' does not exist.")
    return path


def parse_arguments():
    """
    Parse command-line arguments using argparse.

    Returns:
        Namespace: The parsed arguments as a Namespace object.
    """
    parser = argparse.ArgumentParser(
        description="Process the file and other parameters."
    )
    parser.add_argument(
        "file_path", type=file_exists, help="Path of the file to process"
    )
    parser.add_argument(
        "--max_depth",
        type=int,
        default=2,
        help="Maximum depth for processing (default: 2)",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug mode"
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Parse the arguments
    args = parse_arguments()

    FILE_PATH = args.file_path
    MAX_DEPTH = args.max_depth
    DEBUG = args.debug

    # Check if the file exists
    if not FILE_PATH.exists():
        print(f"Error: File not found: {FILE_PATH}")
        sys.exit(1)

    # Main analysis
    final_tree, final_unique_imports = main(FILE_PATH, MAX_DEPTH, DEBUG)

    if DEBUG:
        # If debug is enabled, print detailed results
        print("=== IMPORT TREE ===")
        print_import_tree(final_tree)
        print("=== UNIQUE IMPORTS ===")
        for final_imp in sorted(final_unique_imports):
            print(final_imp)
    else:
        # Print results in a more compact format
        import pprint

        pprint.pprint(final_unique_imports)
        print(final_tree)
