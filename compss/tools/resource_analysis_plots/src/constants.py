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
"""Shared constants and mappings for the profiling CLI."""


# Directories
DEFAULT_STATS_SUBDIR = "runtime_logs/stats"
DEFAULT_PLOTS_DIR = "profiling"

# Scopes for plot generation
SCOPE_MAP = {
    "outliers":   "Outliers Only",
    "aggregated": "Only Aggregated Plots",
    "all_nodes":  "All Nodes",
    "stats_only": "Only Stats Analysis",
}

# Metrics map
METRIC_MAP = {
    "CPU": {
        "col": "CPU",
        "cli_flag": "cpu",
        "unit": "CPU %",
        "is_required": True,
        "is_gpu": False
    },
    "Memory": {
        "col": "MEM",
        "cli_flag": "mem",
        "unit": "Memory %",
        "is_required": True,
        "is_gpu": False
    },
    "GPU": {
        "col": "GPU_USAGE",
        "cli_flag": "gpu",
        "unit": "GPU %",
        "is_required": False,
        "is_gpu": True
    },
    "GPU Memory": {
        "col": "GPU_MEM",
        "cli_flag": "gpu_mem",
        "unit": "GPU Memory (%)",
        "is_required": False,
        "is_gpu": True
    }
}

# Reverse mapping for CLI flags to metric labels
METRIC_FLAGS = {data["cli_flag"]: label for label, data in METRIC_MAP.items()}
# List of all available metrics and required metrics for validation
DEFAULT_METRICS = list(METRIC_MAP.keys())
# List of required metrics
REQUIRED_METRICS = ["TIME"] + [data["col"] for data in METRIC_MAP.values() if data["is_required"]]

# Constants for user interaction and prompts
def get_base_metric_choices() -> list:
    import questionary
    return [
        questionary.Choice("CPU", checked=True),
        questionary.Choice("Memory", checked=True),
    ]

def get_gpu_metric_choices() -> list:
    import questionary
    return [
        questionary.Choice("GPU", checked=True),
        questionary.Choice("GPU Memory", checked=True),
    ]


# Constants plots
# Color-blind friendly palette (Paul Tol's high contrast)
COLOR_PALETTE = [
    '#4477AA', '#EE6677', '#228833', '#CCBB44',
    '#66CCEE', '#AA3377', '#BBBBBB', '#000000'
]
MARKER_STYLES = ['o', 's', '^', 'v', 'D', 'p', '*', 'h', 'H', '+', 'x', '|', '_']
LINE_STYLES = ['-', '--', '-.', ':']


# Constants for statistical analysis and stage classification
MIN_STAGE_PERCENTAGE = 5

STAGE_CONFIG = {
    "CPU": {
        "metric": "CPU",
        "title": "CPU Execution Stages",
        "stages": {
            "unstable": "Unstable / Highly Fluctuating",
            "idle": "Idle / I/O Wait",
            "peak": "Peak Compute",
            "underperforming": "Underperforming / Tailing Off",
            "moderate": "Ramping / Moderate Load",
            "hanging": "Hanging / Prolonged Teardown"
        },
        "descriptions": {
            "default": "Nodes transitioning or rebalancing.",
            "unstable": "Temporary variability across nodes. Monitor if persistent.",
            "hanging": "Extended idle during shutdown. May be normal but check if unexpected.",
            "idle": "Low activity / I/O-bound period.",
            "peak": "High parallel cluster utilization (expected under heavy compute).",
            "underperforming": "Likely start/finalization or uneven workload distribution. Consider optimization."
        },
        "warnings": {
            "underperforming_title": "[bold yellow]Notice: Periods of lower utilization[/bold yellow]",
            "underperforming_desc": (
                "The cluster spent [bold]{duration:.1f}s ({perc:.1f}% of total run)[/bold] in an 'Underperforming / Tailing Off' state.\n"
                "This often happens at the start or end of a run due to workload distribution and can be normal. "
                "It's worth reviewing task distribution and load balancing if you want to improve efficiency."
            ),
            "hanging_title": "[bold yellow]Notice: Extended idle periods detected[/bold yellow]",
            "hanging_desc": (
                "The cluster spent [bold]{duration:.1f}s ({perc:.1f}% of total run)[/bold] in an idle state.\n"
                "This can happen during shutdown or while waiting for I/O. If unexpected, consider checking for "
                "slow shutdowns or synchronization delays."
            ),
            "color": "yellow"
        }
    },
    "GPU": {
        "metric": "GPU_USAGE",
        "title": "GPU Execution Stages",
        "stages": {
            "unstable": "Unstable / GPU Fluctuations",
            "idle": "Idle / Waiting for Data",
            "peak": "Peak GPU Compute",
            "underperforming": "Underperforming / GPU Starvation",
            "moderate": "Work Preparation / Moderate GPU Load",
            "hanging": "Hanging / Prolonged Teardown"
        },
        "descriptions": {
            "default": "GPUs transitioning or rebalancing.",
            "unstable": "Temporary GPU variability. Monitor if persistent.",
            "hanging": "Extended GPU idle during shutdown. May be normal.",
            "idle": "Low GPU activity. May be due to data transfer or setup.",
            "peak": "High parallel GPU utilization (expected during heavy GPU compute).",
            "underperforming": "GPUs active but underused. May be caused by CPU-bound preprocessing."
        },
        "warnings": {
            "underperforming_title": "[bold red]⚠ Warning: GPU Starvation / Underutilization Detected[/bold red]",
            "underperforming_desc": (
                "The cluster spent [bold]{duration:.1f}s ({perc:.1f}% of total run)[/bold] in an 'Underperforming / GPU Starvation' state.\n"
                "During this phase, GPUs were active but utilizing a very small fraction of their computational power. "
                "This indicates a processing bottleneck, likely caused by CPU-bound data preprocessing (starving the GPU), "
                "sub-optimal batch sizes, or straggling GPU tasks."
            ),
            "hanging_title": "[bold red]⚠ Warning: Prolonged GPU Inactivity Detected[/bold red]",
            "hanging_desc": (
                "The GPUs spent [bold]{duration:.1f}s ({perc:.1f}% of total run)[/bold] doing absolutely nothing.\n"
                "This could mean the GPU computation ended prematurely while the CPU continued, a deadlock occurred, "
                "or there was a massive bottleneck in data preparation completely preventing the GPUs from receiving work."
            ),
            "color": "red"
        }
    }
}
