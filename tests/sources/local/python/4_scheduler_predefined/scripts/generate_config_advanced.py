#!/usr/bin/env python3
"""
Advanced Config Generator for Predefined Scheduler

This script analyzes runtime.log to generate a config.json that:
1. Detects computing units (threads) per worker
2. Extracts real dependencies from the log
3. Allows parallelization up to the thread limit
4. Only adds dependencies when truly necessary

Usage:
    python3 generate_config_advanced.py runtime.log > config.json
"""

import re
import json
import sys
import os
from collections import defaultdict
from datetime import datetime


def parse_timestamp(line):
    """Extract timestamp from log line."""
    m = re.match(r'\[\((\d+)\)\(([^\)]+)\)', line)
    if m:
        line_num = int(m.group(1))
        timestamp_str = m.group(2)
        try:
            ts = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
            return line_num, ts
        except:
            return line_num, None
    return None, None


def parse_logs(log_file_path):
    """
    Parse runtime.log and generate intelligent task configuration.
    """
    
    # Data structures
    tasks = {}  # taskId -> {taskId, implementationId, predecessors, resources}
    worker_threads = {}  # worker -> num_computing_units
    task_assignments = {}  # taskId -> {worker, timestamp, line_num}
    task_dependencies = defaultdict(set)  # taskId -> set of predecessor taskIds
    worker_task_timeline = defaultdict(list)  # worker -> [(taskId, line_num, timestamp)]
    
    # Regex patterns
    computing_units_re = re.compile(
        r'Run task received.*"computing_units":(\d+).*on (\S+)'
    )
    
    assign_re = re.compile(
        r'Assigning action \S+ \(Task (\d+),[^\)]+\) to worker (\S+) with implementation (\d+)'
    )
    
    dependency_re = re.compile(
        r'Adding dependency between task (\d+) and task (\d+)'
    )
    
    print("Parsing runtime.log...", file=sys.stderr)
    
    # First pass: extract all information
    with open(log_file_path, 'r') as f:
        for line in f:
            line_num, timestamp = parse_timestamp(line)
            
            # Extract computing units per worker
            m = computing_units_re.search(line)
            if m:
                units = int(m.group(1))
                worker = m.group(2)
                if worker not in worker_threads:
                    worker_threads[worker] = units
                    print(f"  Found {worker}: {units} computing units", file=sys.stderr)
                continue
            
            # Extract task assignments
            m = assign_re.search(line)
            if m:
                task_id = int(m.group(1))
                worker = m.group(2)
                impl_id = int(m.group(3))
                
                if task_id not in tasks:
                    tasks[task_id] = {
                        'taskId': task_id,
                        'implementationId': impl_id,
                        'predecessors': [],
                        '_resources': []
                    }
                
                if worker not in tasks[task_id]['_resources']:
                    tasks[task_id]['_resources'].append(worker)
                
                if task_id not in task_assignments:
                    task_assignments[task_id] = {
                        'worker': worker,
                        'timestamp': timestamp,
                        'line_num': line_num
                    }
                    
                    # Add to timeline
                    worker_task_timeline[worker].append((task_id, line_num, timestamp))
                
                continue
            
            # Extract dependencies
            m = dependency_re.search(line)
            if m:
                pred_task = int(m.group(1))
                succ_task = int(m.group(2))
                task_dependencies[succ_task].add(pred_task)
                continue
    
    print(f"\nFound {len(tasks)} tasks", file=sys.stderr)
    print(f"Found {len(worker_threads)} workers", file=sys.stderr)
    print(f"Found {sum(len(deps) for deps in task_dependencies.values())} dependencies", file=sys.stderr)
    
    # Second pass: calculate intelligent predecessors
    print("\nCalculating intelligent predecessors...", file=sys.stderr)
    
    for worker, timeline in worker_task_timeline.items():
        # Sort by assignment order (line number)
        timeline.sort(key=lambda x: x[1])
        
        max_parallel = worker_threads.get(worker, 1)
        print(f"\n  {worker}: max {max_parallel} parallel tasks", file=sys.stderr)
        
        # Track which tasks are "active" (can run in parallel)
        active_window = []
        
        for i, (task_id, line_num, timestamp) in enumerate(timeline):
            # Get real dependencies from log
            real_deps = task_dependencies.get(task_id, set())
            
            # Find which tasks from this worker should be predecessors
            # A task is a predecessor if:
            # 1. It's a real dependency (from the log), OR
            # 2. The active window is full and this task needs to wait
            
            local_predecessors = []
            
            # Add real dependencies that are on the same worker
            for pred_id in real_deps:
                if pred_id in [t[0] for t in timeline[:i]]:
                    local_predecessors.append(pred_id)
            
            # If we don't have enough slots, add predecessor to enforce ordering
            if len(active_window) >= max_parallel:
                # Need to wait for the oldest task in the window
                oldest_task = active_window[0]
                if oldest_task not in local_predecessors:
                    local_predecessors.append(oldest_task)
                # Remove oldest from window
                active_window.pop(0)
            
            # Add current task to active window
            active_window.append(task_id)
            
            # Update task predecessors (merge with global dependencies)
            if task_id in tasks:
                # Combine local predecessors with global dependencies
                all_preds = set(local_predecessors) | real_deps
                tasks[task_id]['predecessors'] = sorted(list(all_preds))
                
                if local_predecessors:
                    print(f"    Task {task_id}: local_preds={local_predecessors}, "
                          f"real_deps={sorted(real_deps)}, "
                          f"final={sorted(all_preds)}", file=sys.stderr)
    
    # Post-process: convert to final format
    result = []
    for task_id in sorted(tasks.keys()):
        task = tasks[task_id]
        resources = task.pop('_resources', [])
        
        if not resources:
            task['resource'] = 'UNKNOWN_RESOURCE'
        elif len(resources) == 1:
            task['resource'] = resources[0]
        else:
            task['resources'] = resources
        
        result.append(task)
    
    print(f"\nGenerated config with {len(result)} tasks", file=sys.stderr)
    return result


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 generate_config_advanced.py <runtime.log>", file=sys.stderr)
        print("\nThis script generates an intelligent config that:", file=sys.stderr)
        print("  - Detects computing units per worker", file=sys.stderr)
        print("  - Extracts real dependencies from the log", file=sys.stderr)
        print("  - Allows parallelization up to thread limit", file=sys.stderr)
        print("  - Only adds ordering dependencies when necessary", file=sys.stderr)
        sys.exit(1)
    
    log_file = sys.argv[1]
    if not os.path.exists(log_file):
        print(f"Error: File {log_file} not found.", file=sys.stderr)
        sys.exit(1)
    
    data = parse_logs(log_file)
    
    # Output JSON to stdout
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
