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
import typing
from pathlib import Path
from typing import Optional
from typing import Set
from typing import Union


def get_imports_from_file(file_path: Union[str, Path]) -> Set[str]:
    """Parse a Python file and extracts all import statements.

    TIP: including from-imports

    :param file_path: The path to the Python file to analyze.
    :return: A set of fully qualified import names (e.g., 'pandas.core.frame').
    :raises FileNotFoundError: If the file is not found.
    :raises SyntaxError: If the file contains invalid Python syntax.
    :raises UnicodeDecodeError: If there is a problem decoding the file.
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


def resolve_module_path(module_name: str) -> Optional[Path]:
    """Resolve the file system path of a module based on its name.

    :param module_name: The name of the module to resolve (e.g., 'pandas').
    :return: Returns the file path of the module if found, otherwise None.
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


def is_standard_library(path: Path) -> bool:
    """Determine if a given file path belongs to the Python standard library.

    :param path: The path to check.
    :return: True if the path is part of the standard library, False otherwise.
    """
    stdlib_dir = Path(sysconfig.get_paths()["stdlib"])
    return stdlib_dir in path.parents or path == stdlib_dir


def get_imports(
    file_path: Union[str, Path],
    max_depth: int = 2,
    debug: bool = False,
) -> typing.Tuple[typing.Dict, typing.Set]:
    """
    Recursively collects imported modules from a Python file and submodules.

    :param file_path: The Python file to analyze for imports.
    :param max_depth: The maximum recursion depth to follow sub-imports.
    :param debug: Whether to output debug information.
    :return: A tuple containing:
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


def print_import_tree(tree: dict, indent: int = 0):
    """Print the nested structure of imports in a readable format.

    :param tree: A dictionary representing the nested import tree.
    :param indent: The indentation level for each line of output.
    """
    for module, sub in tree.items():
        print("  " * indent + f"- {module}")
        if isinstance(sub, dict):
            print_import_tree(sub, indent + 1)
        elif isinstance(sub, str):
            print("  " * (indent + 1) + sub)


def main(file_path: Union[str, Path], max_depth: int = 2, debug: bool = False):
    """Analyze imports in a given Python file and submodules.

    :param file_path (str or Path): The path to the Python file to analyze.
    :param max_depth (int, optional): Maximum depth for sub-import exploration.
    :param debug (bool, optional): Whether to enable debug mode.
    :return: A tuple containing:
            - result_tree: A dictionary representing the nested import tree.
            - result_unique_imports: A set of unique imports found across
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
    """Check if the given file exist.

    :param path_string: The path to check.
    :return: The Path object corresponding to the file.
    :raises argparse.ArgumentTypeError: If the file does not exist.
    """
    path = Path(path_string)
    if not path.exists():
        raise argparse.ArgumentTypeError(f"The file '{path}' does not exist.")
    return path


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments using argparse.

    :return: Namespace: The parsed arguments as a Namespace object.
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
