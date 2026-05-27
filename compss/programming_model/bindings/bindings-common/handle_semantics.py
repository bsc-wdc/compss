#!/usr/bin/env python3
#
#  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

"""
Generate semantics script that reads from a source directory and creates files in target directory.
"""

import argparse
import os
import sys
import re


def validate_arguments(args):
    """Validate command-line arguments."""
    if not os.path.exists(args.source):
        print(f"Error: Source path '{args.source}' does not exist", file=sys.stderr)
        sys.exit(1)
    
    if not os.path.isdir(args.source):
        print(f"Error: Source path '{args.source}' is not a directory", file=sys.stderr)
        sys.exit(1)
    
    return True


def create_target_directory(target_dir):
    """Create target directory if it doesn't exist."""
    os.makedirs(target_dir, exist_ok=True)
    print(f"Created target directory: {target_dir}")


def parse_java_enum(file_path):
    """
    Parse a Java enum file and extract class name and enum values with their IDs.
    
    Returns a dict with 'enum_class_name' and 'values' (map of value names to IDs).
    Returns None if the file doesn't contain a valid enum.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract enum class name
        enum_class_pattern = r'public\s+enum\s+(\w+)'
        enum_match = re.search(enum_class_pattern, content)
        if not enum_match:
            return None
        enum_class_name = enum_match.group(1)
        
        # Extract enum body (content between first { and last })
        enum_start = content.find('{', enum_match.start())
        enum_end = content.rfind('}')
        if enum_start == -1 or enum_end == -1:
            return None
        
        enum_body = content[enum_start + 1:enum_end]
        
        # Find the end of enum constants (marked by the first semicolon)
        constants_end = enum_body.find(';')
        if constants_end == -1:
            # If no semicolon found, use entire body (unlikely in valid enum)
            constants_section = enum_body
        else:
            # Extract only the constants definition part (before the semicolon)
            constants_section = enum_body[:constants_end]
        
        # Extract enum constants and their first constructor argument (the ID)
        enum_values = {}
        
        # Match enum constants with their arguments
        # Match word characters followed by parentheses with content
        constant_pattern = r'(\w+)\s*\(\s*([^,\)]+)'
        for match in re.finditer(constant_pattern, constants_section):
            constant_name = match.group(1)
            id_value = match.group(2).strip()
            
            # Remove quotes if present
            id_value = id_value.strip('\'"')
            
            # Only include if it looks like an enum constant (starts with uppercase)
            if constant_name[0].isupper():
                enum_values[constant_name] = id_value
        
        return {
            "enum_class_name": enum_class_name,
            "values": enum_values
        }
    except Exception as e:
        print(f"Error parsing {file_path}: {e}", file=sys.stderr)
        return None


def process_source_files(source_path):
    """Process files from source directory and extract enum information."""
    files_map = {}
    
    print(f"Reading files from: {source_path}")
        
    for root, dirs, files in os.walk(source_path):
        # Calculate relative path for better readability
        rel_path = os.path.relpath(root, source_path)
               
        # Process all Java files in current directory
        for file in files:
            if file.endswith('.java'):
                full_file_path = os.path.join(root, file)
                full_rel_path = os.path.join(rel_path, file) if rel_path != "." else file
                # Normalize path separators to forward slashes for consistency
                full_rel_path = full_rel_path.replace(os.sep, "/")
                
                # Parse the Java enum file
                enum_data = parse_java_enum(full_file_path)
                if enum_data:
                    files_map[full_rel_path] = enum_data
                else:
                    files_map[full_rel_path] = {}
    
    return files_map


def generate_semantics_file(files_map, target_file):
    """
    Generate a C header file in target directory based on the parsed enums.
    Creates a single runtime_semantics.h file with all enum definitions.
    """
    print(f"\nGenerating C header file at: {target_file}")
    
    # Create target directory if needed
    target_dir = os.path.dirname(target_file)
    os.makedirs(target_dir, exist_ok=True)
    
    # Generate the C header code from all enums
    header_code = generate_c_header_code(files_map)
    
    # Write the header file
   
    try:
        with open(target_file, 'w', encoding='utf-8') as f:
            f.write(header_code)
        print(f"  Created: {target_file}")
    except Exception as e:
        print(f"  Error writing {target_file}: {e}", file=sys.stderr)


def generate_c_header_code(files_map):
    """
    Generate C header file content from parsed Java enums.
    
    Args:
        files_map: Dictionary of relative paths to enum data
    
    Returns:
        String containing the complete C header file content
    """
    code = "// Autogenerated file (see handle_semantics.py).\n"
    code += "//  - Uses java classes in runtime-commons es/bsc/compss/semantics/ as the source template.\n\n"
    code += "#ifndef RUNTIME_SEMANTICS_H\n"
    code += "#define RUNTIME_SEMANTICS_H\n\n"
    
    # Process each enum and add it to the header
    for rel_path in sorted(files_map.keys()):
        enum_data = files_map[rel_path]
        
        # Skip empty entries
        if not enum_data or 'enum_class_name' not in enum_data:
            continue
        
        enum_class_name = enum_data['enum_class_name']
        enum_values = enum_data['values']
        
        # Generate the enum section in the header
        enum_section = generate_c_enum_section(enum_class_name, enum_values)
        code += enum_section
    
    code += "#endif\n"
    
    return code


def generate_c_enum_section(enum_class_name, enum_values):
    """
    Generate a C preprocessor section for an enum.
    
    Args:
        enum_class_name: Name of the enum class (e.g., "AccessMode")
        enum_values: Dictionary of enum constant names to their IDs
    
    Returns:
        String containing the C preprocessor definitions for this enum
    """
    # Convert enum class name to uppercase with underscores for the prefix
    enum_prefix = convert_to_c_prefix(enum_class_name)
    
    # Create the header guard
    guard_name = f"RUNTIME_SEMANTICS_{enum_prefix}_H"
    
    section = f"\n// {enum_prefix} IDs\n"
    section += f"#ifndef {guard_name}\n"
    section += f"#define {guard_name}\n\n"
    
    # Sort values by ID and create #define statements
    if enum_values:
        try:
            sorted_values = sorted(enum_values.items(), key=lambda x: int(x[1]))
        except ValueError:
            sorted_values = sorted(enum_values.items(), key=lambda x: x[1])
        
        # Find the maximum define name length for alignment
        max_name_length = max(
            len(f"#define {enum_prefix}_{const_name}") 
            for const_name, _ in sorted_values
        )
        
        for const_name, const_id in sorted_values:
            define_name = f"{enum_prefix}_{const_name}"
            # Pad the define with spaces for alignment
            padding = " " * (max_name_length - len(f"#define {define_name}") + 1)
            section += f"#define {define_name}{padding}{const_id}\n"
    
    section += "\n#endif\n"
    
    return section


def convert_to_c_prefix(enum_class_name):
    """
    Convert a Java enum class name to a C preprocessor prefix.
    e.g., "AccessMode" -> "ACCESS_MODE", "StdIOStream" -> "STD_IO_STREAM"
    
    Keeps consecutive uppercase letters together (e.g., "IO" stays as "IO", not "I_O")
    
    Args:
        enum_class_name: The enum class name
    
    Returns:
        The converted C prefix in UPPER_CASE
    """
    result = ""
    for i, char in enumerate(enum_class_name):
        if i > 0 and char.isupper():
            # Insert underscore if:
            # 1. Previous character is lowercase, OR
            # 2. Next character exists and is lowercase (transition from acronym to word)
            prev_is_lower = enum_class_name[i - 1].islower()
            next_is_lower = (i + 1 < len(enum_class_name)) and enum_class_name[i + 1].islower()
            
            if prev_is_lower or next_is_lower:
                result += "_"
        
        result += char
    
    return result.upper()

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate semantics header file"
    )
    parser.add_argument(
        "source",
        help="Source path: directory containing input files"
    )
    
    parser.add_argument(
        "target",
        help="Target File: where generated files will be created"
    )
    
    args = parser.parse_args()
    
    print(f"Generating semantics with source: {args.source} and target: {args.target}")

    # Validate arguments
    validate_arguments(args)
        

    
    # Process source files and extract semantics
    files_map = process_source_files(args.source)
    
    # Generate semantics files in target directory
    generate_semantics_file(files_map, args.target)
        

if __name__ == "__main__":
    main()
