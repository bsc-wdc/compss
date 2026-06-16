# COMPSs Resource Usage Analysis & Plotting Tool

A CLI tool for generating rich profiling plots and statistical reports from COMPSs runtime CSV statistics.

## Overview

This tool reads per-node CSV statistics produced by a COMPSs application run and generates:

- **Individual node plots** — per-metric time-series charts for each node (CPU, Memory, GPU, GPU Memory).
- **Aggregated cluster plots** — multi-node overlaid charts that highlight outliers.
- **Statistical health reports** — cluster-wide analytics with outlier detection, performance extremes, execution-phase inference, and imbalance warnings.

## Features

- **Dual-mode CLI** — interactive wizard-style prompts via [`questionary`](https://questionary.readthedocs.io/) or fully headless execution via command-line flags.
- **Auto-detection of GPU data** — GPU metrics are offered only when GPU columns are present in the CSV files.
- **Smart outlier detection** — uses the IQR method for larger clusters (≥ 5 workers) and falls back to a median-based threshold (±30%) for small clusters.
- **Execution stage inference** — automatically identifies phases such as *Peak Compute*, *Idle / I/O Wait*, *Underperforming / Tailing Off*, *Hanging / Prolonged Teardown*, *Unstable / Highly Fluctuating*, and *Ramping / Moderate Load*.
- **Cluster balance analysis** — detects metric imbalance across nodes using the coefficient of variation (CV).
- **Color-blind friendly palette** — uses Paul Tol's high-contrast colors.
- **Multiple output formats** — SVG (default, 300 DPI), PNG (150 DPI), or JPG (100 DPI, compressed).
- **Rich terminal output** — progress bars, beautifully formatted tables, and warning panels via [`rich`](https://rich.readthedocs.io/).
- **Graceful degradation** — works without `rich` or `questionary` installed when running in headless mode.

## Installation

Install from the project root:

```bash
pip install .
```

### Requirements

- Python >= 3.7
- pandas >= 1.0.0
- matplotlib >= 3.0.0
- questionary >= 2.0.0
- rich >= 13.0.0

Dependencies are automatically installed by `pip`.

## Usage

After installation, the command `compss_genprofiling` is available.

### Interactive Mode

Run without arguments to launch the interactive wizard:

```bash
compss_genprofiling
```

The wizard guides you through four steps:

1. **Data Location** — path to the directory containing your CSV stats files. If a `profiling/stats` folder exists in the current working directory, it is automatically selected.
2. **Action Scope** — choose what to generate:
   - `All Nodes` — plots for every node (master + all workers)
   - `Outliers Only` — plots only for nodes flagged as statistical outliers
   - `Only Aggregated Plots` — skip individual node plots, generate cluster-wide charts only
   - `Only Stats Analysis` — print the statistical report without generating any plots
3. **Metrics Selection** — pick the metrics to analyze (CPU, Memory, GPU, GPU Memory).
4. **Advanced Analytics** — optionally generate the cluster health & statistical report.

### Headless Mode

For automated or scripted usage, pass the required `--dir` flag and optionally `--silent` to skip all interactive prompts:

```bash
compss_genprofiling --dir=/path/to/data --output_dir=/path/to/output --silent --scope=all --metrics=cpu,mem --format=svg
```

#### CLI Arguments

| Flag | Description |
|------|-------------|
| `--dir DIRECTORY` | **(Required)** Path to the directory containing CSV stats files. |
| `--silent` | Run in headless mode without interactive prompts. |
| `--scope SCOPE` | Plot scope: `all`, `outliers`, or `aggregated`. Default: `all`. |
| `--metrics LIST` | Comma-separated metrics: `cpu`, `mem`, `gpu`, `gpu_mem`. Default: all available. |
| `--format FORMAT` | Output format: `svg`, `png`, or `jpg`. Default: `svg`. |
| `--output_dir DIR` | Custom directory for output plots. If omitted, plots are saved inside the data directory. |

#### Examples

Generate all plots silently:
```bash
compss_genprofiling --dir=/path/to/data --silent
```

Generate only aggregated plots in PNG format:
```bash
compss_genprofiling --dir=/path/to/data --silent --scope=aggregated --format=png
```

Analyze only CPU and memory outliers:
```bash
compss_genprofiling --dir=/path/to/data --silent --scope=outliers --metrics=cpu,mem
```

Save plots to a custom directory:
```bash
compss_genprofiling --dir=/path/to/data --silent --output_dir=/path/to/output
```

### Output Structure

All outputs are placed in a `plots/` folder inside the data directory (or the directory specified with `--output_dir`):

```
<data_dir>/
├── plots/
│   ├── <MASTER_NODE>/
│   │   ├── cpu.svg
│   │   └── mem.svg
│   ├── outliers_cpu/<worker_node>/
│   ├── outliers_memory/<worker_node>/
│   ├── outliers_cpu_memory/<worker_node>/
│   ├── other_workers/<worker_node>/
│   ├── cpu_aggregated.svg
│   └── mem_aggregated.svg
```

- **Master node** plots are always saved under `<MASTER_NODE>/`.
- **Outlier workers** are grouped into folders named by the metrics in which they are flagged (e.g., `outliers_cpu`, `outliers_cpu_memory`).
- **Non-outlier workers** are placed under `other_workers/`.
- **Aggregated plots** are saved at the top level of the plots directory.

## Input Format

Each CSV file in the data directory represents one node and must contain at least:

| Column | Description |
|--------|-------------|
| `TIME` | Timestamp column (will be parsed as datetime) |
| `CPU`  | CPU usage percentage |
| `MEM`  | Memory usage percentage |

If GPU data is available, the following columns may also be present:

| Column      | Description          |
|-------------|----------------------|
| `GPU_USAGE` | GPU utilization (%)  |
| `GPU_MEM`   | GPU memory usage (%) |

Files that do not contain the required columns (`TIME`, `CPU`, `MEM`) are silently skipped.

## Architecture

```
compss_genprofiling/
├── pyproject.toml          # Project metadata & entry-point script
├── genprofiling_cli.py     # Thin entry-point wrapper
├── src/
│   ├── __init__.py         # Package init
│   ├── core.py             # Orchestration logic, CLI argument parsing & pipeline
│   ├── interactive.py      # Questionary prompts & interactive configuration
│   ├── plotting.py         # Matplotlib engine (individual + aggregated plots)
│   ├── stats.py            # Outlier detection, stage inference & statistical reporting
│   ├── constants.py        # Color palette, metric mappings & stage configurations
│   └── utils.py            # Rich console wrapper, abort helpers
```
