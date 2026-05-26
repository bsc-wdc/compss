import sys
import json
import os
import re

def main():
    # 1. Argument Parsing
    if len(sys.argv) < 2:
        print("ERROR: Missing provenance directory argument.")
        print("Usage: python3 check_ro_crate_metadata.py <provenance_directory>")
        sys.exit(1)

    prov_dir = sys.argv[1]
    file_path = os.path.join(prov_dir, 'ro-crate-metadata.json')

    print(f"Target directory: {prov_dir}")
    print(f"Target file: {file_path}")

    # 2. File Loading
    if not os.path.exists(file_path):
        print(f"ERROR: The file {file_path} does not exist.")
        sys.exit(1)

    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"ERROR: Failed to parse JSON. Details: {e}")
        sys.exit(1)

    graph = data.get('@graph', [])
    if not graph:
        print("ERROR: '@graph' array is missing or empty in the JSON file.")
        sys.exit(1)

    # 3. Define Expected Metrics
    # Exact matches for method metrics
    exact_metrics = {
        "#overall.matmul_tasks.multiply.maxTime",
        "#overall.matmul_tasks.multiply.executions",
        "#overall.matmul_tasks.multiply.avgTime",
        "#overall.matmul_tasks.multiply.minTime",
        "#overall.matmul_files.py.executionTime"
    }

    # Regex patterns for resource usage metrics
    regex_metrics = [
        re.compile(r"^#.*\.matmul_tasks\.multiply\.maxTime$"),
        re.compile(r"^#.*\.matmul_tasks\.multiply\.executions$"),
        re.compile(r"^#.*\.matmul_tasks\.multiply\.avgTime$"),
        re.compile(r"^#.*\.matmul_tasks\.multiply\.minTime$"),
        re.compile(r"^#.*\.cpuAvg$"),
        re.compile(r"^#.*\.cpuMax$"),
        re.compile(r"^#.*\.memAvg$"),
        re.compile(r"^#.*\.memMin$"),
        re.compile(r"^#.*\.memMax$"),
        re.compile(r"^#.*\.byteSent$"),
        re.compile(r"^#.*\.byteRecv$")
    ]

    # 4. Find the resourceUsage array
    run_action = next((item for item in graph if "resourceUsage" in item), None)
    if not run_action:
        print("ERROR: Could not find any entity containing a 'resourceUsage' array in the @graph.")
        sys.exit(1)

    actual_metric_refs = run_action.get("resourceUsage", [])
    actual_metric_ids = {ref.get("@id") for ref in actual_metric_refs}
    
    print(f"'resourceUsage' contains {len(actual_metric_ids)} metrics.")

    # 5. Validate Exact Metrics
    missing_exact = [m for m in exact_metrics if m not in actual_metric_ids]
    
    if missing_exact:
        print("ERROR: Missing the following method metrics:")
        for m in missing_exact:
            print(f"      - {m}")
        sys.exit(1)
    print("All method metrics FOUND")

    # 6. Validate Regex Metrics
    missing_regex = []
    for pattern in regex_metrics:
        matched = any(pattern.match(metric_id) for metric_id in actual_metric_ids)
        if not matched:
            missing_regex.append(pattern.pattern)
            
    if missing_regex:
        print("ERROR: Missing resource usage metrics:")
        for pattern in missing_regex:
            print(f"      - {pattern}")
        sys.exit(1)
    print("All resource usage metrics FOUND")

    # 7. Validate Metric Definitions in @graph
    graph_ids = {item.get("@id"): item for item in graph if "@id" in item}
    
    validation_errors = False
    for metric_id in actual_metric_ids:
        if metric_id not in graph_ids:
            print(f"ERROR: Metric ID '{metric_id}' is referenced in resourceUsage but missing from the @graph.")
            validation_errors = True
            continue
            
        entity = graph_ids[metric_id]
        if entity.get("@type") != "PropertyValue":
            print(f"ERROR: Entity '{metric_id}' has incorrect @type. Expected 'PropertyValue', got '{entity.get('@type')}'.")
            validation_errors = True
            
        if "name" not in entity:
            print(f"ERROR: Entity '{metric_id}' is missing the 'name' attribute.")
            validation_errors = True
            
        if "value" not in entity:
            print(f"ERROR: Entity '{metric_id}' is missing the 'value' attribute.")
            validation_errors = True

    if validation_errors:
        sys.exit(1)
        
    sys.exit(0)

if __name__ == '__main__':
    main()