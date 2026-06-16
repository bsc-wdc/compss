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

import os
import sys
import time
import subprocess

try:
    import pandas as pd
except ImportError:
    print(
        "PROVENANCE | PROFILING | ERROR: pandas is not installed. Please install it using 'pip install pandas'."
    )
    sys.exit(1)


def get_tool_executable():
    """
    Locates the 'compss_genprofiling' script directly from the COMPSs installation
    to run the code directly instead of relying on the system PATH.
    """
    # Default to /opt/COMPSs if the environment variable is missing
    compss_home = os.environ.get("COMPSS_HOME", "/opt/COMPSs")
    
    # List all the places the script might physically live, prioritizing 
    # the standard installation directories.
    possible_paths = [
        # Standard COMPSs Tools installation path
        os.path.join(compss_home, "Tools", "compss_genprofiling"),
        
        # If it gets installed in a subfolder within Tools
        os.path.join(compss_home, "Tools", "resource_analysis_plots", "genprofiling_cli.py"),
        
        # Source code repository path (for developers running from source)
        os.path.join(compss_home, "compss", "tools", "resource_analysis_plots", "genprofiling_cli.py")
    ]
    
    # Check each path. The moment we find the actual file, we return the command to run it.
    for path in possible_paths:
        if os.path.exists(path):
            print(f"PROVENANCE | PROFILING | Found profiling tool at: {path}")
            # sys.executable ensures we use the exact same Python interpreter currently running
            return [sys.executable, path]
            
    # Ultimate fallback: We couldn't find the file anywhere in COMPSS_HOME.
    # Cross our fingers and hope the user installed it globally in their PATH.
    return ["compss_genprofiling"]


def generate_plots(stats_path) -> str:
    """
    Function to launch the standalone profiling plots generation tool.

    :param stats_path: pathname of the folder containing the data
    :return plots_folder: pathname containing the plots
    """
    start_time = time.time()
    plots_folder = os.path.join(stats_path, "plots")
    
    if not os.path.exists(stats_path):
        print("PROVENANCE | PROFILING | ERROR: stats folder does not exist")
        return None

    try:
        num_csv_files = 0
        gpu_enabled = False

        # Count CSV files (nodes) and check for GPU metrics in the MASTER node
        for fname in os.listdir(stats_path):
            if fname.endswith(".csv") and not os.path.isdir(os.path.join(stats_path, fname)):
                num_csv_files += 1
                if fname.endswith("-MASTER.csv"):
                    fpath = os.path.join(stats_path, fname)
                    # Read only the header to quickly check columns
                    df = pd.read_csv(fpath, nrows=0)
                    if "GPU_USAGE" in df.columns:
                        gpu_enabled = True

        if num_csv_files == 0:
            print("PROVENANCE | PROFILING | WARNING: No CSV files found. Skipping plot generation.")
            return None

        # Construct arguments for the tool
        cmd = get_tool_executable()
        metric_list = "cpu,mem"
        if gpu_enabled:
            metric_list += ",gpu,gpu_mem"

        args = [
            "--dir=" + str(stats_path),
            "--output_dir=" + str(plots_folder),
            "--scope=aggregated",
            "--metrics=" + metric_list,
            "--format=svg",
            "--silent"
        ]
            
        cmd.extend(args)

        # Launch the tool
        print(f"PROVENANCE | PROFILING | Launching plotting tool with command:\n{' '.join(cmd)}")
        subprocess.run(cmd, check=True)

        # replace mode_flag with --all-nodes
        cmd[-4] = "--all-nodes"
        print(f"PROVENANCE | PROFILING | INFO: Only some plots have been generated due to the number of nodes. If you want to generate all the nodes plots launch this command:\n\n{' '.join(cmd)}\n")

        elapsed_time = time.time() - start_time
        print(f"PROVENANCE | PROFILING | Profiling plots generation TIME: {elapsed_time:.2f} s.")

        return plots_folder if os.path.exists(plots_folder) else None

    except subprocess.CalledProcessError as e:
        print(f"PROVENANCE | PROFILING | ERROR: The profiling tool failed with exit code {e.returncode}.")
    except Exception as e:
        print(f"PROVENANCE | PROFILING | ERROR: Could not generate the profiling plots. Exception: {e}")

    return None

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python profiling_plots.py <stats_folder_path>")
        sys.exit(1)

    stats_folder = sys.argv[1]
    generate_plots(stats_folder)
