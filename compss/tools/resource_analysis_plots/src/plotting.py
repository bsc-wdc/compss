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
"""Matplotlib plotting engine."""

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D

from .constants import COLOR_PALETTE, LINE_STYLES, MARKER_STYLES
from .utils import console


def timestamp_axis(time_list):
    step = int(len(time_list) / 60) + 1
    selected_times = time_list[::step]
    labels = [str(time) for time in selected_times]
    time_indices = range(0, len(time_list), step)
    plt.xticks(time_indices, labels=labels, rotation=90)
    plt.subplots_adjust(top=0.95, bottom=0.25)


def build_plot(title, time_list, value_list, name_dataset, measure, avg_val):
    value_list = list(value_list)
    time_list = list(time_list)

    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(12, 8))

    if len(value_list) > 0 and max(value_list) > 10 ** 6:
        value_list = [value / 10 ** 6 for value in value_list]
        avg_val = avg_val / 10 ** 6
        measure = "Megabyte (MB)"
        unit_display = " MB"
    else:
        unit_display = "%"
        
    avg_val = round(avg_val, 1)

    ax.plot(range(len(time_list)), value_list, marker='.', markersize=3, markevery=10, linestyle="-",
            label=name_dataset, color=COLOR_PALETTE[0])
    timestamp_axis(time_list)
    ax.margins(x=0.01, y=0.05)
    plt.axhline(avg_val, color=COLOR_PALETTE[1], linestyle="--", label=f"Average = {avg_val}{unit_display}")

    ax.set_title(title)
    ax.set_xlabel("Timestamp")
    ax.set_ylabel(measure)
    ax.grid(True)
    ax.legend(bbox_to_anchor=(1.005, 1), loc='upper left', borderaxespad=0.1)


def _save_plot(file_path, file_format='svg'):
    """Save the current plot to the specified file path with format-specific optimizations.
    
    :param file_path: The path where the plot should be saved.
    :param file_format: The format to save the plot in (e.g., 'svg', 'png', 'jpg'). Defaults to 'svg'.
    """
    try:
        # Base arguments applied to all formats
        save_kwargs = {
            'format': file_format,
            'bbox_inches': 'tight',
            'pad_inches': 0.1
        }

        # Dynamically apply settings based on the format
        if file_format == 'svg':
            save_kwargs['dpi'] = 300
            
        elif file_format == 'png':
            # Intermediate tradeoff: Decent readability, much faster than 300 DPI
            save_kwargs['dpi'] = 150
            
        elif file_format in ['jpg', 'jpeg']:
            # Superlight: Extremely low resolution and aggressive compression
            save_kwargs['dpi'] = 100
            save_kwargs['pil_kwargs'] = {'quality': 50, 'optimize': True}

        # Save the plot by unpacking the customized arguments
        plt.savefig(file_path, **save_kwargs)
        
    except Exception as e:
        console.print(f"[bold red]WARNING:[/bold red] Could not save plot to {file_path}. {e}")
    finally:
        plt.close()

def build_plot_nodes(resampled_dfs, name_plot, metric, name_metric, outliers=None, file_format='svg'):
    plt.style.use("ggplot")
    plt.figure(figsize=(18, 8))

    outlier_nodes = [out["node"] for out in outliers] if outliers else []
    legend_elements_outliers, legend_elements_normal = [], []

    num_items = len(resampled_dfs)

    for i, (label, resampled_df) in enumerate(resampled_dfs.items()):
        if metric not in resampled_df.columns:
            continue

        is_outlier = label in outlier_nodes
        color = COLOR_PALETTE[i % len(COLOR_PALETTE)]
        marker = MARKER_STYLES[i % len(MARKER_STYLES)]
        linestyle = LINE_STYLES[i % len(LINE_STYLES)]

        # Highlight logic: Make outliers bold, opaque, and drawn on top
        if num_items > 20:
            linewidth = 2.5 if is_outlier else 1.0
            alpha = 1.0 if is_outlier else 0.4
            zorder = 10 if is_outlier else 1
        else:
            linewidth = 2.5 if is_outlier else 2.0
            alpha = 1.0 if is_outlier else 0.8
            zorder = 10 if is_outlier else 1

        x_data = resampled_df.index.to_numpy()
        y_data = resampled_df[metric].to_numpy()

        plt.plot(x_data, y_data, label=label,
                 color=color, marker=marker, markersize=5 if is_outlier else 3,
                 markevery=15, linestyle=linestyle, linewidth=linewidth,
                 alpha=alpha, zorder=zorder)

        line_element = Line2D([0], [0], color=color, marker=marker, linestyle=linestyle,
                              label=label, markersize=8 if is_outlier else 5, linewidth=linewidth)
        (legend_elements_outliers if is_outlier else legend_elements_normal).append(line_element)

    all_times = pd.concat(list(resampled_dfs.values())).index.unique().sort_values()
    if len(all_times) > 0:
        step = int(len(all_times) / 60) + 1
        selected_times = all_times[::step]
        plt.xticks(selected_times, labels=[t.strftime("%Y-%m-%d %H:%M:%S") for t in selected_times], rotation=90)

    plt.subplots_adjust(top=0.95, bottom=0.12, left=0.06, right=0.92)
    plt.xlabel("Timestamp")
    plt.ylabel(f"{name_metric} usage (%)")
    plt.title(f"{name_metric} usage among the nodes")

    leg = plt.legend(handles=legend_elements_outliers + legend_elements_normal,
                     bbox_to_anchor=(1.005, 1), loc='upper left',
                     ncol=max(1, (len(resampled_dfs) + 39) // 40), fontsize='small')

    for text_obj in leg.get_texts():
        if text_obj.get_text() in outlier_nodes:
            text_obj.set_weight('bold')
            text_obj.set_backgroundcolor('#FEFFB5')

    plt.grid(True)
    plt.tight_layout()
    _save_plot(name_plot, file_format)
