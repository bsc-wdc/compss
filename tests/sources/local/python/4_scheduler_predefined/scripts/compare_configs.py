#!/usr/bin/env python3
"""
Compare two config.json files for the Predefined Scheduler test.

This script compares the expected config.json with the actual config
extracted from runtime.log and reports differences.

Usage:
    python3 compare_configs.py expected.json actual.json
"""

import sys
import json


def load_json(filepath):
    """Load and parse JSON file."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {filepath}: {e}", file=sys.stderr)
        return None


def compare_tasks(expected, actual):
    """
    Compare two lists of tasks and return differences.
    
    Returns:
        (bool, list): (all_match, list_of_differences)
    """
    differences = []
    
    # Create dictionaries indexed by taskId for easier comparison
    expected_dict = {task['taskId']: task for task in expected}
    actual_dict = {task['taskId']: task for task in actual}
    
    # Check for missing tasks
    expected_ids = set(expected_dict.keys())
    actual_ids = set(actual_dict.keys())
    
    missing_in_actual = expected_ids - actual_ids
    extra_in_actual = actual_ids - expected_ids
    
    if missing_in_actual:
        differences.append(f"Tasks missing in actual: {sorted(missing_in_actual)}")
    
    if extra_in_actual:
        differences.append(f"Extra tasks in actual: {sorted(extra_in_actual)}")
    
    # Compare common tasks
    common_ids = expected_ids & actual_ids
    
    for task_id in sorted(common_ids):
        exp_task = expected_dict[task_id]
        act_task = actual_dict[task_id]
        
        # Compare implementationId
        if 'implementationId' in exp_task and 'implementationId' in act_task:
            if exp_task['implementationId'] != act_task['implementationId']:
                differences.append(
                    f"Task {task_id}: implementationId mismatch - "
                    f"expected {exp_task['implementationId']}, got {act_task['implementationId']}"
                )
        
        # Compare resource/resources
        # Handle both 'resource' (single) and 'resources' (array) fields
        exp_resources = None
        act_resources = None
        
        if 'resource' in exp_task:
            exp_resources = {exp_task['resource']}
        elif 'resources' in exp_task:
            exp_resources = set(exp_task['resources'])
        
        if 'resource' in act_task:
            act_resources = {act_task['resource']}
        elif 'resources' in act_task:
            act_resources = set(act_task['resources'])
        
        if exp_resources is not None and act_resources is not None:
            if exp_resources != act_resources:
                differences.append(
                    f"Task {task_id}: resource(s) mismatch - "
                    f"expected {sorted(exp_resources)}, got {sorted(act_resources)}"
                )
        
        # Compare predecessors (order doesn't matter)
        if 'predecessors' in exp_task and 'predecessors' in act_task:
            exp_pred = set(exp_task['predecessors'])
            act_pred = set(act_task['predecessors'])
            
            if exp_pred != act_pred:
                differences.append(
                    f"Task {task_id}: predecessors mismatch - "
                    f"expected {sorted(exp_pred)}, got {sorted(act_pred)}"
                )
    
    return len(differences) == 0, differences


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 compare_configs.py expected.json actual.json", file=sys.stderr)
        sys.exit(1)
    
    expected_file = sys.argv[1]
    actual_file = sys.argv[2]
    
    # Load both files
    expected = load_json(expected_file)
    actual = load_json(actual_file)
    
    if expected is None or actual is None:
        sys.exit(1)
    
    # Compare
    all_match, differences = compare_tasks(expected, actual)
    
    if all_match:
        print("✓ Configurations match perfectly!")
        print(f"  - {len(expected)} tasks validated")
        sys.exit(0)
    else:
        print("✗ Configurations differ:")
        for diff in differences:
            print(f"  - {diff}")
        sys.exit(1)


if __name__ == "__main__":
    main()
