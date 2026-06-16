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
"""Statistical analysis and reporting functions."""

import pandas as pd

try:
    from rich import box
    from rich.panel import Panel
    from rich.table import Table
    _RICH_AVAILABLE = True
except ImportError:
    _RICH_AVAILABLE = False
    box = None
    Panel = None
    Table = None

from .constants import METRIC_MAP, MIN_STAGE_PERCENTAGE, STAGE_CONFIG
from .utils import console

def _merge_short_stages(raw_stages, run_dur):
    """Iteratively merges short stages (< min_percent) into their largest adjacent neighbor."""
    if not raw_stages:
        return []
    
    # Combine adjacent stages with the EXACT SAME name
    pass1 = []
    for stage_name, start, end, i_avg, i_max, i_min in raw_stages:
        duration = (end - start).total_seconds()
        
        if not pass1:
            pass1.append([stage_name, start, end, i_avg * duration, i_max, i_min, duration])
        else:
            last = pass1[-1]
            # if the stage name is the same as the last one, merge them directly without checking duration (they are already the same stage)
            if last[0] == stage_name:
                last[2] = end
                last[3] += (i_avg * duration)
                last[4] = max(last[4], i_max)
                last[5] = min(last[5], i_min)
                last[6] += duration
            # if the stage name is different, we check the duration to decide if we keep it separate or merge it into the previous one
            else:
                pass1.append([stage_name, start, end, i_avg * duration, i_max, i_min, duration])
                
    # Iteratively absorb the smallest stage if it is < min_percent
    while True:
        # calculate the percentage of total run duration for each stage
        percents = [(p[6] / run_dur) * 100 for p in pass1]
        min_p = min(percents)

        # id the smallest stage is above the threshold, or we only have 1 stage left, we are done
        if min_p >= MIN_STAGE_PERCENTAGE or len(pass1) == 1:
            break

        # find the index of the smallest stage
        min_idx = percents.index(min_p)
        
        # Decide which neighbor to merge into (prefer the longer neighbor)
        # if the smallest one is the first, merge it with the second one
        if min_idx == 0:
            target_idx = 1
        # if the smallest one is the last, merge it with the second to last one
        elif min_idx == len(pass1) - 1:
            target_idx = min_idx - 1
        else:
            # if the smallest is in the middle, merge it with the neighbor that has the longest duration (largest percentage of run duration)
            if pass1[min_idx - 1][6] >= pass1[min_idx + 1][6]:
                target_idx = min_idx - 1
            else:
                target_idx = min_idx + 1
                
        # Merge the small stage (s) into the target stage (t)
        t = pass1[target_idx]
        s = pass1[min_idx]

        # Update the target stage with the merged values
        t[1] = min(t[1], s[1])
        t[2] = max(t[2], s[2])
        t[3] += s[3]
        t[4] = max(t[4], s[4])
        t[5] = min(t[5], s[5])
        t[6] += s[6]

        # remove the small stage from the list
        pass1.pop(min_idx)
        
        # recompresses adjacent stage intervals that now have the same stage name after a previous merge
        merged_pass1 = []
        for p in pass1:
            # if it is the first stage, just add it to the merged list
            if not merged_pass1:
                merged_pass1.append(p)
            else:
                # get the last stage
                last = merged_pass1[-1]
                # if the name of the current stage is the same as the last one, merge them in one stage
                if last[0] == p[0]:
                    last[2] = p[2]
                    last[3] += p[3]
                    last[4] = max(last[4], p[4])
                    last[5] = min(last[5], p[5])
                    last[6] += p[6]
                # if they are different, just add the current stage to the merged list
                else:
                    merged_pass1.append(p)
        pass1 = merged_pass1
            
    # Convert back to standard tuples, finalizing the weighted average
    return [(p[0], p[1], p[2], p[3] / p[6] if p[6] > 0 else 0, p[4], p[5]) for p in pass1]

def _is_master_orchestrator(node_name, df):
    """Determine if a node is strictly an orchestrator (Master with < 3% avg CPU)."""
    if "MASTER" in node_name.upper():
        if 'CPU' in df.columns:
            return df['CPU'].mean() < 3.0
        return True  # Default to orchestrator if no CPU data is present on a MASTER node
    return False

def _normalize_resampled_dfs(resampled_dfs, metric=None):
    """Normalize resampled data into a node -> DataFrame mapping.

    :param resampled_dfs: The input data which can be in various formats (dict, DataFrame, Series).
    :param metric: Optional metric name to use as the column name if input is a Series or DataFrame without named columns.

    :return: A dictionary mapping node names to DataFrames with a consistent column for the metric.
    """
    if resampled_dfs is None or len(resampled_dfs) == 0:
        return {}

    # if it's already in the correct format, return as is
    if isinstance(resampled_dfs, pd.DataFrame):
        if metric is not None and metric in resampled_dfs.columns:
            node_name = getattr(resampled_dfs, "name", None) or "node"
            return {str(node_name): resampled_dfs}

        normalized = {}
        for node_name, series in resampled_dfs.items():
            column_name = metric or series.name or "value"
            normalized[str(node_name)] = pd.DataFrame({column_name: series})
        return normalized

    # if it's a Series, wrap it in a DataFrame with the metric as the column name
    if isinstance(resampled_dfs, pd.Series):
        node_name = getattr(resampled_dfs, "name", None) or "node"
        column_name = metric or node_name or "value"
        return {str(node_name): pd.DataFrame({column_name: resampled_dfs})}

    return resampled_dfs

def identify_time_series_outliers(resampled_dfs, metric, iqr_multiplier=1.5):
    """
    Identifies nodes that show outlier behavior using the IQR method.

    :param resampled_dfs: Dictionary of node -> DataFrame with the metric data.
    :param metric: The metric column to analyze (e.g., "CPU").
    :param iqr_multiplier: The multiplier for the IQR to define outlier thresholds (default 1.5).

    :return: list of outlier nodes with their total metric value and difference from cluster average,
    """
    resampled_dfs = _normalize_resampled_dfs(resampled_dfs, metric)

    if not resampled_dfs:
        return []

    # Isolate compute workers by excluding the MASTER node from the baseline
    worker_nodes = {}
    for node, df in resampled_dfs.items():
        if metric not in df.columns:
            continue
        if _is_master_orchestrator(node, df):
            continue
        worker_nodes[node] = df[metric].sum()

    # Fallback in case there are no workers (e.g. they only profiled a master node)
    if not worker_nodes:
        worker_nodes = {node: df[metric].sum() for node, df in resampled_dfs.items() if metric in df.columns}

    if not worker_nodes:
        return []

    # convert the node totals into a Series for easier statistical calculations
    total_usage = pd.Series(worker_nodes)
    # Calculate cluster-wide statistics
    cluster_mean = total_usage.mean()
    cluster_median = total_usage.median()

    # calculate the IQR and bounds for outlier detection
    Q1 = total_usage.quantile(0.25)
    Q3 = total_usage.quantile(0.75)
    IQR = Q3 - Q1

    actual_outliers = []
    
    # if there are less than 5 worker nodes, the IQR method can be too lenient, so we use a median-based threshold instead
    if len(worker_nodes) < 5:
        # For small clusters (< 5 workers), IQR mathematical bounds are often too wide.
        # Fallback to absolute median deviation (+/- 30% from the median)
        lower_bound_active = cluster_median * 0.70
        upper_bound_active = cluster_median * 1.30
        # series of booleans indicating which nodes are outliers based on the median threshold
        outliers_mask = (total_usage < lower_bound_active) | (total_usage > upper_bound_active)
    else:
        # Standard IQR for larger node clusters
        lower_bound_active = Q1 - (iqr_multiplier * IQR)
        upper_bound_active = Q3 + (iqr_multiplier * IQR)
        # series of booleans indicating which nodes are outliers based on the IQR method
        outliers_mask = (total_usage < lower_bound_active) | (total_usage > upper_bound_active)

    for node, total in total_usage.items():
        if outliers_mask[node]:
            outlier_item = {
                "node": node,
                "total": total,
                "difference_from_avg": ((total - cluster_mean) / cluster_mean * 100) if cluster_mean > 0 else 0
            }
            actual_outliers.append(outlier_item)

    return actual_outliers


def _display_global_cluster_balance(resampled_dfs, selected_metrics, run_duration, global_warnings):
    """
    Display the cluster balance overview and node extremes.
    
    :param resampled_dfs: DataFrame with resampled metrics.
    :param selected_metrics: List of metrics to analyze.
    :param run_duration: Total run duration in seconds.
    :param global_warnings: List to store any global warnings.
    """
    # build the overview table with mean and imbalance (CV) for each metric, flagging any with high imbalance (>20% CV)
    overview_table = Table(title="Global Performance Balance", box=box.SIMPLE_HEAVY, title_style="bold cyan")
    overview_table.add_column("Metric", style="cyan", justify="left")
    overview_table.add_column("Cluster Mean", style="green", justify="right")
    overview_table.add_column("Imbalance (CV)", style="red", justify="right")
    overview_table.add_column("Run Duration", style="yellow", justify="right")

    imbalanced_metrics = []

    for ui_metric in selected_metrics:
        col = METRIC_MAP[ui_metric]["col"]
        unit = METRIC_MAP[ui_metric]["unit"]
        # normalize the dataframes to ensure we have a consistent node
        normalized_dfs = _normalize_resampled_dfs(resampled_dfs, col)

        valid_means = [df[col].mean() for df in normalized_dfs.values() if col in df.columns]
        if not valid_means:
            continue

        overall_mean = pd.Series(valid_means).mean()
        overall_std = pd.Series(valid_means).std()
        cv = (overall_std / overall_mean * 100) if overall_mean > 0 else 0

        if cv > 20.0:
            imbalanced_metrics.append((ui_metric, cv))

        display_unit = "%" if "%" in unit else "MB"
        overview_table.add_row(ui_metric, f"{overall_mean:.2f} {display_unit}", f"{cv:.1f}%", f"{run_duration:.1f} sec")

    console.print(overview_table)
    console.print()

    if imbalanced_metrics:
        imbalance_details = "\n".join([f"- [bold]{m}[/bold]: {c:.1f}% CV" for m, c in imbalanced_metrics])
        global_warnings.append(Panel(
            f"[bold yellow]Notice: Cluster imbalance observed[/bold yellow]\n"
            f"The following metrics show higher-than-expected variation across nodes (CV > 20%):\n{imbalance_details}\n"
            f"This can result from uneven data distribution or task placement. It may be acceptable for some workloads, but reviewing task scheduling or data partitioning can help improve balance.",
            border_style="yellow"
        ))

    extremes_table = Table(title="Node Extremes (Averages)", box=box.SIMPLE_HEAVY, title_style="bold cyan")
    extremes_table.add_column("Metric", style="cyan")
    extremes_table.add_column("Hardest Worker (Max)", style="red")
    extremes_table.add_column("Laziest Node (Min)", style="blue")

    for ui_metric in selected_metrics:
        col = METRIC_MAP[ui_metric]["col"]
        normalized_dfs = _normalize_resampled_dfs(resampled_dfs, col)
        means = pd.Series({node: df[col].mean() for node, df in normalized_dfs.items() if col in df.columns})

        if means.empty:
            continue

        max_node, max_val = means.idxmax(), means.max()
        min_node, min_val = means.idxmin(), means.min()

        unit = "%" if "%" in METRIC_MAP[ui_metric]["unit"] else "MB"
        extremes_table.add_row(
            ui_metric,
            f"[bold]{max_node}[/bold] ({max_val:.1f}{unit})",
            f"[bold]{min_node}[/bold] ({min_val:.1f}{unit})"
        )

    console.print(extremes_table)
    console.print()


def _display_outliers_section(selected_metrics, outliers_dict):
    """Display the outlier table."""
    # build the table
    outliers_table = Table(title="Identified Outliers (IQR Method)", box=box.SIMPLE_HEAVY, title_style="bold red")
    outliers_table.add_column("Node Name", style="white")
    outliers_table.add_column("Metric Flagged", style="cyan")
    outliers_table.add_column("Difference from Avg", style="magenta")
    outliers_table.add_column("Behavior", justify="center")

    has_outliers = False
    # iterate on every selected metric
    for ui_metric in selected_metrics:
        col = METRIC_MAP[ui_metric]["col"]
        # iterate on every outlier in the list for the specific metric column
        if col in outliers_dict:
            for outlier in outliers_dict[col]:
                has_outliers = True
                behavior = "High Outlier" if outlier["difference_from_avg"] > 0 else "Low Outlier"
                diff_value = outlier['difference_from_avg']
                style = "red" if diff_value > 0 else "blue"
                outliers_table.add_row(
                    f"[bold]{outlier['node']}[/bold]",
                    ui_metric,
                    f"[{style}]{diff_value:.1f}%[/{style}]",
                    behavior
                )

    if has_outliers:
        console.print(outliers_table)
    else:
        console.print("[green]✔ The cluster performed homogeneously. No statistical outliers detected.[/green]")
        console.print()


def _append_resource_stage_warning(resource_type, run_duration, threshold_perc, underperforming_duration, hanging_duration, global_warnings):
    """Generic warning appender to avoid repeating logic."""
    cfg = STAGE_CONFIG[resource_type]["warnings"]

    if underperforming_duration > 0:
        perc = (underperforming_duration / run_duration) * 100
        if underperforming_duration > 15.0 and perc > threshold_perc:
            global_warnings.append(Panel(
                f"{cfg['underperforming_title']}\n{cfg['underperforming_desc'].format(duration=underperforming_duration, perc=perc)}",
                border_style=cfg["color"]
            ))

    if hanging_duration > 0:
        perc = (hanging_duration / run_duration) * 100
        if hanging_duration > 15.0 and perc > threshold_perc:
            global_warnings.append(Panel(
                f"{cfg['hanging_title']}\n{cfg['hanging_desc'].format(duration=hanging_duration, perc=perc)}",
                border_style=cfg["color"]
            ))

def _display_resource_execution_stages(resource_type, resampled_dfs, selected_metrics, run_duration, global_warnings):
    """Generic execution stages logic to process both CPU and GPU metrics."""
    cfg = STAGE_CONFIG[resource_type]
    metric = cfg["metric"]

    # Pre-check for GPU specific logic to maintain exact original behavior
    if resource_type == "GPU" and ("GPU" not in (selected_metrics or []) or not any(metric in df.columns for df in resampled_dfs.values())):
        return

    resampled_dfs = _normalize_resampled_dfs(resampled_dfs, metric)

    # get the df of usage across the nodes
    resource_df = pd.DataFrame({node: df[metric] for node, df in resampled_dfs.items() if metric in df.columns})
    if resource_df.empty:
        return

    # initialize variables for stage detection
    stages = []
    current_stage = None
    start_time = None
    avg_vals = []
    history_means = []

    # get the average of every node and then the average among all the nodes
    global_avg = resource_df.mean().mean()
    active_threshold = max(global_avg * 0.5, 10.0)

    # get the time and the usage of every row in the dataframe
    for time, row in resource_df.iterrows():
        max_val, min_val, mean_val = row.max(), row.min(), row.mean()
        total_nodes = len(row)

        # if the max value is above the active threshold, we consider the cluster to be computing
        is_computing = max_val >= active_threshold
        node_spread = max_val - min_val

        # maintain a short history of mean values to detect possible instability over time
        history_means.append(mean_val)
        if len(history_means) > 3:
            # every iteration remove the oldest one to keep only the last 3
            history_means.pop(0)
        # calculate the temporal difference in mean values over the history window to detect instability
        temporal_diff = max(history_means) - min(history_means) if len(history_means) == 3 else 0

        # Stage classification logic using mapped names
        # if the cluster is computing but there is a large spread between nodes or high temporal variability, we consider it unstable
        if is_computing and (node_spread > 50.0 or temporal_diff > 50.0):
            stage = cfg["stages"]["unstable"]
        # if the cluster is not computing and the max usage is very low, we consider it idle
        elif not is_computing and max_val < 5.0:
            stage = cfg["stages"]["idle"]
        # if the average usage is above 60% or at least half of the nodes are above 60%, we consider it a peak stage
        elif mean_val > 60.0 or (row > 60.0).sum() >= (total_nodes // 2):
            stage = cfg["stages"]["peak"]
        # if the cluster is computing but the average usage is low and there is not a large spread between nodes, we consider it underperforming
        elif is_computing and mean_val < 25.0 and max_val < 60.0:
            stage = cfg["stages"]["underperforming"]
        # in all the other cases, we consider it a moderate stage
        else:
            stage = cfg["stages"]["moderate"]

        # when we detect a stage change, we finalize the previous stage interval and start a new one
        if current_stage != stage:
            if current_stage is not None:
                interval_avg = sum(avg_vals) / len(avg_vals) if avg_vals else 0
                interval_max = max(avg_vals) if avg_vals else 0
                interval_min = min(avg_vals) if avg_vals else 0
                # append the stage interval to the list of stages with the average, max and min usage during that interval
                stages.append((current_stage, start_time, time, interval_avg, interval_max, interval_min))

            current_stage = stage
            start_time = time
            # reset the average values for the new stage interval
            avg_vals = [mean_val]
        else:
            avg_vals.append(mean_val)

    # Process final segment
    if current_stage is not None:
        interval_avg = sum(avg_vals) / len(avg_vals) if avg_vals else 0
        stages.append((current_stage, start_time, resource_df.index[-1], interval_avg, max(avg_vals) if avg_vals else 0, min(avg_vals) if avg_vals else 0))

    # Construct the rendering table
    timeline_table = Table(title=cfg["title"], box=box.SIMPLE_HEAVY, title_style="bold yellow")
    timeline_table.add_column("Stage Phase", style="bold cyan")
    timeline_table.add_column("Start Time", style="white")
    timeline_table.add_column("End Time", style="white")
    timeline_table.add_column("Duration%", justify="right", style="green")
    timeline_table.add_column(f"Avg {resource_type}%", justify="right", style="magenta")
    timeline_table.add_column(f"Max {resource_type}%", justify="right", style="red")
    timeline_table.add_column(f"Min {resource_type}%", justify="right", style="blue")
    timeline_table.add_column("Description", style="dim")

    # Initialize accumulators for potential warnings about underperforming and hanging stages
    underperforming_duration = 0
    hanging_duration = 0
    teardown_threshold = max(run_duration * 0.02, 5.0)
    mid_run_threshold = max(run_duration * 0.05, 10.0)

    # Before rendering, merge any short stages into their neighbors to avoid cluttering the timeline with very brief intervals
    merged_stages = _merge_short_stages(stages, run_duration)

    for i, (stage, start, end, interval_avg, interval_max, interval_min) in enumerate(merged_stages):
        duration = (end - start).total_seconds()
        duration_percent = (duration / run_duration) * 100
        is_last_stage = (i == len(merged_stages) - 1)

        desc = cfg["descriptions"]["default"]

        # if the stage is unstable, we use the specific description for unstable stages
        if cfg["stages"]["unstable"] in stage:
            desc = cfg["descriptions"]["unstable"]
        # if the stage is idle, we check if it is the last stage and if its duration is above the threshold to consider it a hanging stage
        elif stage == cfg["stages"]["idle"]:
            if is_last_stage and duration >= teardown_threshold:
                stage = cfg["stages"]["hanging"]
                desc = cfg["descriptions"]["hanging"]
                hanging_duration += duration
            else:
                desc = cfg["descriptions"]["idle"]
                if duration >= mid_run_threshold:
                    hanging_duration += duration
        elif stage == cfg["stages"]["peak"]:
            desc = cfg["descriptions"]["peak"]
        elif stage == cfg["stages"]["underperforming"]:
            desc = cfg["descriptions"]["underperforming"]
            underperforming_duration += duration

        timeline_table.add_row(
            stage, start.strftime("%H:%M:%S"), end.strftime("%H:%M:%S"),
            f"{duration_percent:.1f}%", f"{interval_avg:.1f}%", f"{interval_max:.1f}%", f"{interval_min:.1f}%", desc
        )

    console.print(timeline_table)
    console.print()

    # define the threshold percentage for the warning messages based on the total run duration
    if run_duration < 300:
        threshold_perc = 15.0
    elif run_duration < 900:
        threshold_perc = 10.0
    elif run_duration < 1800:
        threshold_perc = 5.0
    else:
        threshold_perc = 3.0

    # show warnings based on the accumulated durations and the thresholds defined for different run durations
    _append_resource_stage_warning(resource_type, run_duration, threshold_perc, underperforming_duration, hanging_duration, global_warnings)


def display_statistical_analysis(resampled_dfs, selected_metrics, outliers_dict, run_duration, number_of_nodes):
    """Generates a highly detailed, creative statistical report with execution stages."""
    if not _RICH_AVAILABLE:
        console.print("[yellow]Warning: rich not installed. Skipping statistical analysis display.[/yellow]")
        return

    console.print()
    console.rule("[bold magenta]📊 Cluster Health & Advanced Analytics Report[/bold magenta]")
    console.print()

    global_warnings = []  # Accumulate all warnings to print at the very end
    
    if number_of_nodes > 1:
        _display_global_cluster_balance(resampled_dfs, selected_metrics, run_duration, global_warnings)

    _display_outliers_section(selected_metrics, outliers_dict)

    _display_resource_execution_stages("CPU", resampled_dfs, None, run_duration, global_warnings)

    _display_resource_execution_stages("GPU", resampled_dfs, selected_metrics, run_duration, global_warnings)

    # Print all collected warnings at the very end
    if global_warnings:
        console.rule("[bold red]⚠ Global Execution Warnings[/bold red]")
        console.print()
        for warning in global_warnings:
            console.print(warning)
            console.print()
    else:
        console.print("[green]✔ No significant warnings detected. Cluster performance appears healthy.[/green]")
        console.print()
