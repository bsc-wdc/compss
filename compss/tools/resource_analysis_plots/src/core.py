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
"""Orchestration logic and CLI entry point."""

import os
import time
from pathlib import Path
from contextlib import nullcontext
import sys

import pandas as pd
import argparse

from .utils import console

try:
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    _RICH_AVAILABLE = True
except ImportError:
    _RICH_AVAILABLE = False

from .constants import METRIC_MAP, SCOPE_MAP, DEFAULT_METRICS, METRIC_FLAGS, REQUIRED_METRICS, DEFAULT_STATS_SUBDIR, DEFAULT_PLOTS_DIR
from .plotting import _save_plot, build_plot, build_plot_nodes
from .stats import display_statistical_analysis, identify_time_series_outliers


def _load_csv_files(data_dir: str, print_message: bool, selected_metrics: list):
    """
    Load CSV files from the specified directory.

    :param data_dir: Directory containing the CSV files.
    :param print_message: Whether to print progress messages.
    :param selected_metrics: List of metrics to check for in the CSV files.
    :return: A tuple containing the list of dataframes, list of machine names, and the maximum length of the dataframes.
    """    
    df_list = []
    name_list = []
    max_length = 0

    # We need the context manager to wrap the process of loading CSV files (used in the rich progress display)
    if print_message:
        ctx_mgr = console.status("[bold green]Reading CSV files...", spinner="dots")
    else:
        ctx_mgr = nullcontext()
    
    already_warned = False
    with ctx_mgr:
        for fname in os.listdir(data_dir):
            if fname.endswith(".csv"):
                file_path = os.path.join(data_dir, fname)
                machine_name = fname.split(".csv")[0].split("_")[-1]
                df = pd.read_csv(file_path)

                if not set(REQUIRED_METRICS).issubset(set(df.columns)):
                    continue

                # check if the selected metrics are included in the df
                required_cols = [METRIC_MAP[m]["col"] for m in selected_metrics if m in METRIC_MAP]
                diff = set(required_cols) - set(df.columns)
                
                if diff and not already_warned:
                    console.print(f"[yellow]⚠  Warning: Selected metrics not found in CSV. Missing columns: {', '.join(diff)}. Related plots will be skipped.[/yellow]")
                    already_warned = True

                    # Actively remove the missing metrics from the selected_metrics list
                    # Modifying it in-place using [:] ensures the changes propagate to execute_generation
                    selected_metrics[:] = [m for m in selected_metrics if METRIC_MAP.get(m, {}).get("col") not in diff]

                df_list.append(df)
                name_list.append(machine_name)
                max_length = max(max_length, len(df))
    
    return df_list, name_list, max_length

def _resample_datasets(
        df_list: list, 
        name_list: list, 
        print_message: bool
    ):
    """Resample the datasets to a common time base.
    
    :param df_list: List of dataframes, each containing the metrics for a node.
    :param name_list: List of node names corresponding to the dataframes.
    :param print_message: Whether to print progress messages.
    :return: A tuple containing the dictionary of resampled dataframes for each node and the total run duration in seconds.
    """
    if print_message:
        ctx_mgr = console.status("[bold green]Resampling data and computing stats...", spinner="dots")
    else:
        ctx_mgr = nullcontext()

    with ctx_mgr:
        for df in df_list:
            df["TIME"] = pd.to_datetime(df["TIME"])

        # calculate the global min and max time across all dataframes
        min_time = min(df["TIME"].min() for df in df_list)
        max_time = max(df["TIME"].max() for df in df_list)
        run_duration = (max_time - min_time).total_seconds()

        # calculate the interval
        interval_s = None
        for df in df_list:
            if len(df) > 1:
                # sort the dataframe based on the time
                df_sorted = df.sort_values("TIME")
                # median() replaces mean() to ignore large gaps/crashes
                interval_s = int(round(df_sorted["TIME"].diff().median().total_seconds()))
                break
        
        # align the dataframe
        aligned_dfs = {}
        if interval_s and interval_s > 0:
            common_grid = pd.date_range(start=min_time, end=max_time, freq=f"{interval_s}s")
            
            for df, label in zip(df_list, name_list):
                df_working = df.copy().set_index("TIME")
                df_working = df_working[~df_working.index.duplicated(keep='first')]
                
                aligned_dfs[label] = df_working.reindex(
                    common_grid, 
                    method='nearest', 
                    tolerance=pd.Timedelta(seconds=interval_s//2)
                )

                # set the indeces of the first and last valid data (not NaN)
                first_idx = aligned_dfs[label].first_valid_index()
                last_idx = aligned_dfs[label].last_valid_index()
                
                # If the dataframe has data, fill internal NaNs with the latest known value
                if first_idx is not None and last_idx is not None:
                    aligned_dfs[label].loc[first_idx:last_idx] = aligned_dfs[label].loc[first_idx:last_idx].ffill()

                aligned_dfs[label].index.name = "TIME"
        else:
            for df, label in zip(df_list, name_list):
                aligned_dfs[label] = df.copy().set_index("TIME")

    return aligned_dfs, run_duration

def _outlier_detection(
        resampled_dfs: dict, 
        selected_metrics: list, 
    ):
    """Identify outliers for each metric across the nodes.
    
    :param resampled_dfs: Dictionary of resampled dataframes for each node.
    :param selected_metrics: List of metrics to analyze (e.g., CPU, Memory).
    :return: dict containing outliers node with node, timestamp and value for each metric
    """
    outlier_dict = {}
    for ui_metric in selected_metrics:
        try:
            col = METRIC_MAP[ui_metric]["col"]
        except KeyError:
            # Handle the case where the UI metric is not in the METRIC_MAP
            continue
        outlier_dict[col] = identify_time_series_outliers(resampled_dfs, col)
    return outlier_dict

def _is_node_outlier(
    node_name: str,
    metric_col: str,
    outliers_dict: dict,
) -> bool:
    # fetch metric outliers
    outliers = outliers_dict.get(metric_col, [])
    return any(node_name == out["node"] for out in outliers)

def _get_number_of_plots(
        df_list: list, 
        name_list: list, 
        selected_metrics: list, 
        plot_scope: str, 
        outliers_dict: dict
    ) -> int:
    """Calculate the total number of plots to generate based on the plot scope and outlier information.
    
    :param df_list: List of dataframes, each containing the metrics for a node.
    :param name_list: List of node names corresponding to the dataframes.
    :param selected_metrics: List of metrics to plot (e.g., CPU, Memory).
    :param plot_scope: Scope of plotting (e.g., "All Nodes", "Outliers Only", "Only Aggregated Plots").
    :param outliers_dict: Dictionary containing outlier information for each metric, where the key is the metric column and the value is a tuple of (actual_outliers, q1, q3, upper). actual_outliers is a list of dictionaries with keys "node", "timestamp" and "value".
    :return: Total number of plots to generate.
    """
    total_plots = 0
    single_node_run = len(df_list) == 1

    if plot_scope in ["All Nodes", "Outliers Only"] or single_node_run:
        # iterate on each node to check if we need to plot it based on the scope and count the number of plots to generate
        for df, machine_name in zip(df_list, name_list):
            is_master = "MASTER" in machine_name.upper()

            if single_node_run or is_master or plot_scope == "All Nodes":
                total_plots += sum(1 for ui_metric in selected_metrics if METRIC_MAP[ui_metric]["col"] in df.columns)
            elif plot_scope == "Outliers Only":
                # if the node is an outlier in any metric, count all its available plots
                is_outlier = any(
                    _is_node_outlier(machine_name, METRIC_MAP[m]["col"], outliers_dict)
                    for m in METRIC_MAP
                    if "col" in METRIC_MAP[m]
                )

                if is_outlier:
                    total_plots += sum(1 for m in selected_metrics if METRIC_MAP[m]["col"] in df.columns)

    return total_plots

def _get_plot_output_path(
        machine_name: str,
        plots_dir: str,
        plot_scope: str,
        single_node_run: bool,
        is_master: bool,
        node_outlier_metrics: list,
    ):
    """
    Determine whether to plot a node and the output path for its plots.
    
    :param machine_name: Name of the machine/node.
    :param plots_dir: Base directory where plots should be saved.
    :param plot_scope: Scope of plotting (e.g., "All Nodes", "Outliers Only", "Only Aggregated Plots").
    :param single_node_run: Whether this is a single-node run.
    :param is_master: Whether the machine is a master node.
    :param node_outlier_metrics: List of metrics in which this node is an outlier.
    """
    # if single node or master, always plot it in the main directory
    if single_node_run:
        return True, plots_dir

    if is_master:
        return True, os.path.join(plots_dir, machine_name)
    
    plot_node = False
    # for other workers, we check the scope and the outlier status
    if plot_scope == "All Nodes":
        plot_node = True
    elif plot_scope == "Outliers Only" and len(node_outlier_metrics) > 0:
        plot_node = True

    # if the node is not in the scope to be plotted, return False and None
    if not plot_node:
        return False, None
    
    # create scalable, dynamic folders based on the metrics in which it is an outlier
    if node_outlier_metrics:
        # e.g., ["CPU", "GPU Memory"] becomes "cpu_gpu_memory", then sorted alphabetically
        clean_metrics = [str(m).lower().replace(" ", "_") for m in node_outlier_metrics]
        clean_metrics.sort()
        folder_name = "outliers_" + "_".join(clean_metrics)
        output_path = os.path.join(plots_dir, folder_name, machine_name)
    else:
        output_path = os.path.join(plots_dir, "other_workers", machine_name)

    return True, output_path

def _generate_individual_plots(
        df_list: list,
        name_list: list,
        resampled_dfs: dict,
        selected_metrics: list,
        outliers_dict: dict,
        plot_scope: str,
        plots_dir: str,
        plot_format: str,
        max_length: int,
        advance_plot=None,
    ):
    """
    Generate individual plots for each node based on the provided data and configuration.

    :param df_list: List of dataframes, each containing the metrics for a node.
    :param name_list: List of node names corresponding to the dataframes.
    :param resampled_dfs: Dictionary of resampled dataframes for each node.
    :param selected_metrics: List of metrics to plot (e.g., CPU, Memory).
    :param outliers_dict: Dictionary containing outlier information for each metric.
    :param plot_scope: Scope of plotting (e.g., "All Nodes", "Outliers Only").
    :param plots_dir: Base directory where plots should be saved.
    :param plot_format: Format for saving plots (e.g., "svg", "png").
    :param max_length: Maximum length of the dataframes (used for sampling).
    :param advance_plot: Optional callback function to advance the progress bar after each plot is generated.
    """
    # single node if len is 1
    single_node_run = len(df_list) == 1
    # to avoid plotting too many points, we sample the dataframe to have at most 100 points (this is needed for very long runs with high frequency data collection)
    global_step = max(1, max_length // 100)

    # iterate on each node and plot it if it is in the scope
    for df, machine_name in zip(df_list, name_list):
        is_master = "MASTER" in machine_name.upper()

        # Collect a list of all metrics where this node is an outlier
        node_outlier_metrics = []
        for m in selected_metrics:
            if m in METRIC_MAP:
                col = METRIC_MAP[m]["col"]
                if _is_node_outlier(machine_name, col, outliers_dict):
                    node_outlier_metrics.append(m)

        # Get the output path dynamically
        plot_it, output_path = _get_plot_output_path(
            machine_name=machine_name,
            plots_dir=plots_dir,
            plot_scope=plot_scope,
            single_node_run=single_node_run,
            is_master=is_master,
            node_outlier_metrics=node_outlier_metrics,
        )

        if not plot_it:
            continue

        os.makedirs(output_path, exist_ok=True)

        # Sample the dataframe to avoid plotting too many points
        df_sampled = df.iloc[::global_step, :].copy()
        timestamps = df_sampled["TIME"]

        for ui_metric in selected_metrics:
            col = METRIC_MAP[ui_metric]["col"]
            unit = METRIC_MAP[ui_metric]["unit"]
            file_name = f"{col.lower()}.{plot_format}"

            # Skip if the metric column is not present in this node's data
            if col not in df_sampled.columns:
                continue

            node_avg = resampled_dfs[machine_name][col].mean()
            title = f"{ui_metric} usage of {machine_name}"

            build_plot(title, timestamps, df_sampled[col], ui_metric, unit, node_avg)
            _save_plot(os.path.join(output_path, file_name), plot_format)

            if advance_plot is not None:
                advance_plot()

def _generate_aggregated_plots(
        resampled_dfs: dict, 
        plots_dir: str, 
        selected_metrics: list, 
        outliers_dict: dict, 
        plot_format: str,
        advance_plot=None,
    ):
    """
    Generate aggregated plots based on the provided data and configuration.

    :param resampled_dfs: Dictionary of resampled dataframes for each node.
    :param plots_dir: Base directory where plots should be saved.
    :param selected_metrics: List of metrics to plot (e.g., CPU, Memory).
    :param outliers_dict: Dictionary containing outlier information for each metric.
    :param plot_format: Format for saving plots (e.g., 'svg', 'png').
    :param advance_plot: Optional callback function to advance the progress bar after each plot is generated.
    """
    # iterate on each metric to build the aggregated plot
    for ui_metric in selected_metrics:
        col = METRIC_MAP[ui_metric]["col"]
        metric_outliers = outliers_dict[col]
        file_name = f"{col.lower()}_aggregated.{plot_format}"
        output_path = os.path.join(plots_dir, file_name)

        build_plot_nodes(
            resampled_dfs,
            output_path,
            col,
            ui_metric,
            outliers=metric_outliers,
            file_format=plot_format
        )

        # advance the progress bar after each plot if the callback is provided
        if advance_plot is not None:
            advance_plot()


def _run_plotting_with_progress(
        total_plots: int,
        df_list: list,
        plot_scope: str,
        print_message: bool,
        generate_individual_plots,
        generate_aggregated_plots,
    ):
    """
    Run the plotting process with a progress bar.

    :param total_plots: Total number of plots to generate.
    :param df_list: List of dataframes, each containing the metrics for a node.
    :param plot_scope: Scope of the plots to generate.
    :param print_message: Whether to print progress messages.
    :param generate_individual_plots: Function to generate individual node plots.
    :param generate_aggregated_plots: Function to generate aggregated plots.
    """
    multiple_nodes = len(df_list) > 1
    single_node_run = not multiple_nodes

    progress = None
    if print_message and total_plots > 0 and _RICH_AVAILABLE:
        progress = Progress(
            SpinnerColumn(style="green"),
            TextColumn("[bold green]{task.description}"),
            BarColumn(bar_width=None),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=True,
        )

    if progress:
        with progress:
            plot_task = progress.add_task("Generating plots", total=total_plots)
            advance_plot = lambda: progress.update(plot_task, advance=1)

            if plot_scope in ["All Nodes", "Outliers Only"] or single_node_run:
                if print_message:
                    desc = (
                        "Generating single-node plots"
                        if single_node_run and plot_scope == "Only Aggregated Plots"
                        else f"Generating individual node plots ({plot_scope})"
                    )
                    progress.update(plot_task, description=desc)
                generate_individual_plots(advance_plot)

            if multiple_nodes:
                if print_message:
                    progress.update(
                        plot_task, description="Generating aggregated cluster plots"
                    )
                generate_aggregated_plots(advance_plot)
    else:
        # No progress bar: fall back to console.status or plain calls.
        if plot_scope in ["All Nodes", "Outliers Only"] or single_node_run:
            if print_message:
                if single_node_run and plot_scope == "Only Aggregated Plots":
                    with console.status(
                        "[bold green]Generating single-node plots...", spinner="dots"
                    ):
                        generate_individual_plots()
                else:
                    with console.status(
                        f"[bold green]Generating individual node plots ({plot_scope})...",
                        spinner="dots",
                    ):
                        generate_individual_plots()
            else:
                generate_individual_plots()

        if multiple_nodes:
            if print_message:
                with console.status(
                    "[bold green]Generating aggregated cluster plots...", spinner="dots"
                ):
                    generate_aggregated_plots()
            else:
                generate_aggregated_plots()

def execute_generation(config: dict):
    """
    This is the pipeline:
    1. Load CSV files
    2. Resample data to a common time base
    3. Identify outliers per metrics
    4. Show statistical report if requested
    5. Generate the individual nodes and the aggregated plots based on the plot_scope
    6. Return path to the plots directory (or None if only stats)

    :param config: Dictionary containing the configuration for the generation process, including:
        - data_dir: Directory containing the CSV files with the metrics.
        - selected_metrics: List of metrics to analyze and plot (e.g., CPU, Memory).
        - plot_scope: Scope of plotting (e.g., "All Nodes", "Outliers Only", "Only Aggregated Plots", "Only Stats Analysis").
        - format: (Optional) Format for saving plots (default: "svg").
        - print_message: (Optional) Whether to print progress messages (default: True).
        - print_stats: (Optional) Whether to print the statistical analysis report (default: True).
    """
    data_dir = plot_dir = config["data_dir"]
    selected_metrics = config["selected_metrics"]
    plot_scope = config["plot_scope"]
    plot_format = config.get("format", "svg").lstrip(".")
    print_message = config.get("print_message", True)
    print_stats = config.get("print_stats", True)
    plot_dir = config["output_plots_dir"]

    # 1. Load the csv files
    df_list, name_list, max_length = _load_csv_files(data_dir, print_message, selected_metrics)
    multiple_nodes = len(df_list) > 1

    if not df_list:
        # print an error and stop if no valid csv found
        console.print("[bold red]✘[/bold red] No valid CSV files found in the target directory. Please ensure that the directory contains CSV files with the appropriate metrics columns.")
        sys.exit(1)
    
    # 2. Resample data to a common time base
    resampled_dfs, run_duration = _resample_datasets(
        df_list, 
        name_list, 
        print_message
    )
    
    # 3. Identify outliers per metric
    outliers_dict = _outlier_detection(
        resampled_dfs, 
        selected_metrics, 
    )

    # 4. Show statistical report if requested
    if print_stats:
        display_statistical_analysis(
            resampled_dfs,
            selected_metrics,
            outliers_dict,
            run_duration,
            len(df_list)
        )

    # early exit if user only wants stats analysis
    if plot_scope == "Only Stats Analysis":
        return None
    
    # ensure the output directory exists
    os.makedirs(plot_dir, exist_ok=True)
    
    # 5. Generate the individual nodes and the aggregated plots based on the plot_scope
    total_plots = _get_number_of_plots(df_list, name_list, selected_metrics, plot_scope, outliers_dict)

    # in order to show the loading bar, we need to define the inner function that we can call with the progress update callback
    def generate_individual_plots_with_progress(advance_plot=None):
        _generate_individual_plots(
            df_list, 
            name_list, 
            resampled_dfs, 
            selected_metrics, 
            outliers_dict, 
            plot_scope,
            plot_dir,
            plot_format, 
            max_length,
            advance_plot
        )

    # generate the aggregated plots if there are more than 1 node
    def generate_aggregated_plots_with_progress(advance_plot=None):
        if multiple_nodes:
            _generate_aggregated_plots(
                resampled_dfs, 
                plot_dir, 
                selected_metrics, 
                outliers_dict, 
                plot_format, 
                advance_plot
            )
    
    # run the plotting with the progress bar
    _run_plotting_with_progress(
        total_plots=total_plots,
        df_list=df_list,
        plot_scope=plot_scope,
        print_message=print_message,
        generate_individual_plots=generate_individual_plots_with_progress,
        generate_aggregated_plots=generate_aggregated_plots_with_progress,
    )

    # 6. Return path to the plots directory
    return plot_dir

def _parse_cli_args() -> argparse.Namespace:
    """Parse command-line arguments shared by interactive and headless modes."""
    parser = argparse.ArgumentParser(description="COMPSs Profiling Plot Generator")

    parser.add_argument(
        "--dir",
        dest="data_dir",
        type=str,
        required=True,
        metavar="DIRECTORY",
        help="Path to the directory containing CSV stats files. Example: --dir=/path/to/data"
    )

    parser.add_argument(
        "--silent",
        action="store_true",
        help="Run in headless mode without interactive prompts."
    )

    parser.add_argument(
        "--scope",
        type=str,
        choices=["all", "outliers", "aggregated"],
        default=None,
        metavar="SCOPE",
        help="Scope of plot generation: 'all', 'outliers', or 'aggregated'. "
             "Example: --scope=all"
    )

    parser.add_argument(
        "--metrics",
        type=lambda s: [m.strip() for m in s.split(",")],
        default=None,
        metavar="METRIC[,METRIC,...]",
        help="Comma-separated list of metrics to include. "
             "Valid values: cpu, mem, gpu, gpu_mem. "
             "Example: --metrics=cpu,mem,gpu_mem"
    )

    parser.add_argument(
        "--format",
        type=str,
        choices=["svg", "png", "jpg"],
        default="svg",
        metavar="FORMAT",
        help="Output format for plots. Valid values: svg, png, jpg (default: svg). "
             "Example: --format=png"
    )

    parser.add_argument(
        "--output_dir",
        type=str,
        default=None,
        metavar="OUTPUT_DIR",
        help="Path to the directory where plots will be saved. It will be created if it doesn't exist."
    )

    args = parser.parse_args()

    # Validate metrics values
    if args.metrics is not None:
        # valid_metrics = {"cpu", "mem", "gpu", "gpu_mem"}
        invalid = set(args.metrics) - set(METRIC_FLAGS.keys())
        if invalid:
            parser.error(
                f"Invalid metric(s): {', '.join(sorted(invalid))}. "
                f"Valid options are: {', '.join(sorted(METRIC_FLAGS.keys()))}."
            )
    else:
        args.metrics = list(DEFAULT_METRICS)

    return args

def _resolve_scope(args: argparse.Namespace) -> str:
    # Safely get args.scope, fallback to 'all_nodes'
    scope_key = getattr(args, 'scope', 'all_nodes') 
    label = SCOPE_MAP.get(scope_key, SCOPE_MAP["all_nodes"])
    
    return label

def _resolve_metrics(args: argparse.Namespace) -> list[str]:
    """Map the provided CLI metric flags to their internal UI labels."""
    # Create a dictionary to map short CLI flags to UI labels (e.g., 'mem' -> 'Memory')    
    metric_list = getattr(args, "metrics", None)

    if not metric_list:
        return list(DEFAULT_METRICS)

    selected = []
    for metric in metric_list:
        if metric in METRIC_FLAGS:
            selected.append(METRIC_FLAGS[metric])
        else:
            # Fallback just in case the label is already correctly formatted
            selected.append(metric) 
            
    return selected if selected else list(DEFAULT_METRICS)

def _get_headless_config(args: argparse.Namespace) -> dict:
    """Build configuration for headless (non-interactive) plot generation.

    :param args: Parsed command-line arguments.
    :raises IsADirectoryError: If the specified data directory does not exist.
    :return: Configuration dictionary for plot generation.
    """
    data_dir = output_plots_dir = args.data_dir

    if data_dir == DEFAULT_STATS_SUBDIR:
        output_plots_dir = DEFAULT_PLOTS_DIR

    if not os.path.isdir(data_dir):
        raise IsADirectoryError(
            f"Directory not found or is not a directory: '{data_dir}'"
        )

    return {
        "data_dir":         data_dir,
        "print_stats":      False,
        "print_message":    False,
        "plot_scope":       _resolve_scope(args),
        "selected_metrics": _resolve_metrics(args),
        "format":           args.format,
        "output_plots_dir": output_plots_dir
    }

def _get_user_config_interactive(predefined_path: str | None):
    """
    Import interactive mode lazily so that headless --silent runs
    without requiring 'questionary' (and its dependencies).
    """
    try:
        from .interactive import get_user_config
    except ImportError as exc:
        raise RuntimeError(
            "Interactive mode requires the optional dependency 'questionary' "
            "and its UI stack. Install them or run with --silent."
        ) from exc

    return get_user_config(predefined_path=predefined_path)

def _show_success_banner(plots_dir: str, elapsed: float):
    console.print()
    if plots_dir:
        msg = (
            f"[bold green]✔  Profiling completed successfully in {elapsed:.2f}s![/bold green]\n"
            f"Plots saved to: [cyan]{Path(plots_dir).resolve()}[/cyan]"
        )
    else:
        msg = (
            f"[bold green]✔  Statistical analysis completed in {elapsed:.2f}s![/bold green]\n"
            f"Plotting was skipped by user request."
        )

    if _RICH_AVAILABLE:
        console.print(Panel.fit(msg, border_style="green"))
    else:
        console.print(msg)

def main():
    try:
        args = _parse_cli_args() if len(sys.argv) > 1 else None

        # Toggle between Headless and Interactive Mode based on CLI arguments
        if args and args.silent:
            # No questionary nor rich
            config = _get_headless_config(args)
        else:
            predefined_path = args.data_dir if args else None
            config = _get_user_config_interactive(predefined_path=predefined_path)
            if args:
                config["format"] = args.format

        if args and args.output_dir:
            config["output_plots_dir"] = args.output_dir

        # check if it is silent mode or not
        silent_mode = not config.get("print_message", True)
        
        # start the generation process and measure the time taken
        start_time = time.time()
        if not silent_mode:
            console.print()

        # execute the generation pipeline and get the output directory for the plots (if any)
        plots_dir = execute_generation(config)
        elapsed = time.time() - start_time

        # Success Banner
        if not silent_mode:
            _show_success_banner(plots_dir, elapsed)

    except KeyboardInterrupt:
        _abort()
