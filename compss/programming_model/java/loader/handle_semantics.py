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


def generate_semantics_files(files_map, target_dir):
    """
    Generate Java enum files in target directory based on the files map.
    Creates files in es.bsc.compss.runtime package with the same relative path structure.
    """
    print(f"\nGenerating Java semantics files in: {target_dir}")
    
    for rel_path, enum_data in files_map.items():
        # Skip empty entries (files that weren't valid enums)
        if not enum_data or 'enum_class_name' not in enum_data:
            continue
        
        # Remove the .java extension if present
        path_without_extension = rel_path.replace('.java', '')
        
        # Get the directory path (without the filename)
        dir_relative_path = os.path.dirname(path_without_extension)
        file_name = os.path.basename(path_without_extension) + ".java"
        
        # Build the target file path
        if dir_relative_path:
            package_path = f"es/bsc/compss/loader/runtime/{dir_relative_path}"
            target_file_path = os.path.join(target_dir, package_path, file_name)
        else:
            package_path = "es/bsc/compss/loader/runtime"
            target_file_path = os.path.join(target_dir, package_path, file_name)
        
        # Create parent directories if needed
        os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
        
        # Generate Java enum code
        enum_class_name = enum_data['enum_class_name']
        enum_values = enum_data['values']
        
        # Build the package name from only the directory path (not including filename)
        if dir_relative_path:
            package_name = f"es.bsc.compss.loader.runtime.{dir_relative_path.replace(os.sep, '.')}"
        else:
            package_name = "es.bsc.compss.loader.runtime"
        package_name = package_name.replace('/', '.')
        
        # Generate the Java enum code
        java_code = generate_java_enum_code(enum_class_name, enum_values, package_name)
        
        # Write the Java enum file
        try:
            with open(target_file_path, 'w', encoding='utf-8') as f:
                f.write(java_code)
            print(f"  Created: {target_file_path}")
        except Exception as e:
            print(f"  Error writing {target_file_path}: {e}", file=sys.stderr)


def generate_java_enum_code(enum_class_name, enum_values, package_name):
    """
    Generate Java enum source code.
    
    Args:
        enum_class_name: Name of the enum class
        enum_values: Dictionary of enum constant names to their IDs
        package_name: Full package name
    
    Returns:
        String containing the complete Java enum code
    """
    # Start with package and class declaration
    code = f"package {package_name};\n\n"
    code += f"public enum {enum_class_name} {{\n"
    
    # Add enum constants sorted by ID
    if enum_values:
        # Sort by ID value (convert to int for proper numeric sorting)
        try:
            sorted_values = sorted(enum_values.items(), key=lambda x: int(x[1]))
        except ValueError:
            # If IDs are not numeric, sort alphabetically by ID
            sorted_values = sorted(enum_values.items(), key=lambda x: x[1])
        
        constants = []
        for const_name, const_id in sorted_values:
            constants.append(f"    {const_name}({const_id}), //")
        code += "\n".join(constants) + "\n"
        code += ";\n"
    
    # Add private field and constructor
    code += "\n    private final byte id;\n\n"
    code += f"    {enum_class_name}(int id) {{\n"
    code += "        this.id = (byte) id;\n"
    code += "    }\n\n"
    
    # Add getter method
    code += "    public byte getID() {\n"
    code += "        return id;\n"
    code += "    }\n"
    
    code += "}\n"
    
    return code

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate semantics files"
    )
    parser.add_argument(
        "source",
        help="Source path: directory containing input files"
    )
    
    parser.add_argument(
        "target",
        help="Target directory: where generated files will be created or cleaned"
    )
    
    args = parser.parse_args()
    
    print(f"Generating semantics with source: {args.source} and target: {args.target}")

    # Validate arguments
    validate_arguments(args)
        
    # Create target directory
    create_target_directory(args.target)
    
    # Process source files and extract semantics
    files_map = process_source_files(args.source)
    
    # Generate semantics files in target directory
    generate_semantics_files(files_map, args.target)
        

if __name__ == "__main__":
    main()
