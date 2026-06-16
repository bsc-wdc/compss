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
"""Interactive user prompts and configuration."""

import os
import sys

import pandas as pd
import questionary
from rich.panel import Panel

from .constants import DEFAULT_PLOTS_DIR, DEFAULT_STATS_SUBDIR, SCOPE_MAP, get_base_metric_choices, get_gpu_metric_choices
from .utils import _abort, _ask, console


def _print_banner() -> None:
    console.print(
        Panel.fit(
            "[bold cyan]COMPSs Profiling Plot Generator[/bold cyan]\n"
            "[dim]Interactive Profiling & Analytics CLI[/dim]",
            border_style="cyan",
        )
    )

def _resolve_data_dir(predefined_path: str | None) -> str:
    """Return a validated absolute path to the data directory."""
    # If a path was explicitly provided, validate and return it.
    if predefined_path is not None:
        data_dir = os.path.expanduser(predefined_path)
        if not os.path.isdir(data_dir):
            console.print(
                f"[bold red]✘[/bold red] Directory '{predefined_path}' does not exist. Aborting."
            )
            sys.exit(1)
        return data_dir, data_dir

    # If no path was provided, try to auto-detect the candidate path.
    candidate = os.path.join(os.getcwd(), DEFAULT_STATS_SUBDIR)
    if os.path.isdir(candidate):
        return candidate, DEFAULT_PLOTS_DIR  # Directly return instead of falling into the loop

    # If neither predefined nor candidate worked, prompt the user.
    while True:
        raw = _ask(
            lambda: questionary.path(
                "Path to the directory containing the CSV stats files:",
                only_directories=True,
            ).ask()
        )
        if raw is None:
            continue
        data_dir = os.path.expanduser(raw.strip())
        if os.path.isdir(data_dir):
            return data_dir, data_dir
        console.print("[bold red]✘[/bold red] Directory does not exist. Please try again.")

def _detect_dataset_features(data_dir: str) -> bool:
    """
    Scan CSV files in *data_dir* and return whether GPU data is present.

    Exits with an error message when no CSV files are present.
    """
    has_gpu = False
    valid_csvs = False

    for fname in os.listdir(data_dir):
        if not fname.endswith(".csv"):
            continue
        valid_csvs = True
        df_head = pd.read_csv(os.path.join(data_dir, fname), nrows=0)
        if "GPU_USAGE" in df_head.columns:
            has_gpu = True
            break

    if not valid_csvs:
        console.print("[bold red]✘[/bold red] No CSV files found in the target directory.")
        sys.exit(1)

    if has_gpu:
        console.print("[bold green]✔[/bold green] GPU data detected in the datasets.")

    return has_gpu

def _ask_plot_scope() -> str:
    """Prompt the user to select the scope of plot generation and analysis."""
    console.rule("[bold]Step 2 · Action Scope[/bold]")
    return _ask(
        lambda: questionary.select(
            "Select the scope of plot generation and analysis:",
            choices=SCOPE_MAP.values(),
        ).ask()
    )

def _ask_metrics(has_gpu: bool) -> list[str]:
    """Prompt the user to select which metrics to analyze."""
    console.rule("[bold]Step 3 · Metrics Selection[/bold]")
    choices = get_base_metric_choices() + (get_gpu_metric_choices() if has_gpu else [])
    selected = _ask(
        lambda: questionary.checkbox(
            "Select the metrics you want to analyze:",
            choices=choices,
        ).ask()
    )
    if not selected:
        console.print("[bold yellow]⚠ No metrics selected. Exiting.[/bold yellow]")
        sys.exit(0)
    return selected

def _ask_print_stats(plot_scope: str) -> bool:
    """Prompt the user to decide whether to print the statistical cluster health report."""
    if plot_scope == "Only Stats Analysis":
        return True
    console.rule("[bold]Step 4 · Advanced Analytics[/bold]")
    return _ask(
        lambda: questionary.confirm(
            "Would you like to generate a statistical cluster health report?",
            default=True,
        ).ask()
    )

# public function to get user configuration
def get_user_config(predefined_path: str | None = None) -> dict:
    """Interactively collect all profiling configuration from the user."""
    _print_banner()

    console.rule("[bold]Step 1 · Data Location[/bold]")
    data_dir, output_plots_dir = _resolve_data_dir(predefined_path)

    console.print("[dim]Analyzing dataset...[/dim]")
    has_gpu = _detect_dataset_features(data_dir)

    plot_scope = _ask_plot_scope()
    selected_metrics = _ask_metrics(has_gpu)
    print_stats = _ask_print_stats(plot_scope)

    return {
        "data_dir": data_dir,
        "has_gpu": has_gpu,
        "plot_scope": plot_scope,
        "selected_metrics": selected_metrics,
        "print_stats": print_stats,
        "format": "svg",
        "output_plots_dir": output_plots_dir,
    }
