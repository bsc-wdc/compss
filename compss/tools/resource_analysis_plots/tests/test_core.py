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
"""Unit tests for src/core.py."""

import argparse
import sys
from io import StringIO
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src import core
from src.constants import DEFAULT_METRICS, METRIC_MAP


class TestLoadCsvFiles:
    def test_load_csv_files_success(self, tmp_path):
        df = pd.DataFrame({
            "TIME": pd.date_range("2024-01-01", periods=5, freq="s"),
            "CPU": [10.0] * 5,
            "MEM": [20.0] * 5,
        })
        df.to_csv(tmp_path / "stats_node1.csv", index=False)

        df_list, name_list, max_length = core._load_csv_files(str(tmp_path), False, ["CPU", "Memory"])
        assert len(df_list) == 1
        assert name_list == ["node1"]
        assert max_length == 5

    def test_load_csv_files_skips_invalid(self, tmp_path):
        df_bad = pd.DataFrame({"A": [1, 2, 3]})
        df_bad.to_csv(tmp_path / "bad.csv", index=False)

        df_good = pd.DataFrame({
            "TIME": pd.date_range("2024-01-01", periods=5, freq="s"),
            "CPU": [10.0] * 5,
            "MEM": [20.0] * 5,
        })
        df_good.to_csv(tmp_path / "stats_node1.csv", index=False)

        df_list, name_list, max_length = core._load_csv_files(str(tmp_path), False, ["CPU", "Memory"])
        assert len(df_list) == 1
        assert name_list == ["node1"]
        assert max_length == 5
    
    def test_load_csv_files_gpu_warning(self, tmp_path):
        df = pd.DataFrame({
            "TIME": pd.date_range("2024-01-01", periods=5, freq="s"),
            "CPU": [10.0] * 5,
            "MEM": [20.0] * 5,
        })
        df.to_csv(tmp_path / "stats_node1.csv", index=False)

        with patch.object(core, "console") as mock_console:
            core._load_csv_files(str(tmp_path), True, ["GPU", "GPU Memory"])
            # GPU metrics requested but not found, so a warning should be printed
            mock_console.print.assert_called()

    def test_load_csv_with_multiple_files(self, tmp_path):
        df1 = pd.DataFrame({
            "TIME": pd.date_range("2024-01-01", periods=5, freq="s"),
            "CPU": [10.0] * 5,
            "MEM": [20.0] * 5,
        })
        df1.to_csv(tmp_path / "stats_node1.csv", index=False)

        df2 = pd.DataFrame({
            "TIME": pd.date_range("2024-01-01", periods=5, freq="s"),
            "CPU": [15.0] * 5,
            "MEM": [25.0] * 5,
        })
        df2.to_csv(tmp_path / "stats_node2.csv", index=False)

        df_list, name_list, max_length = core._load_csv_files(str(tmp_path), False, ["CPU", "Memory"])
        assert len(df_list) == 2
        assert set(name_list) == {"node1", "node2"}
        assert max_length == 5


class TestResampleDatasets:
    def test_resample_basic(self):
        now = pd.Timestamp("2024-01-01")
        df1 = pd.DataFrame({
            "TIME": pd.date_range(now, periods=5, freq="s"),
            "CPU": [10.0] * 5,
        })
        df2 = pd.DataFrame({
            "TIME": pd.date_range(now + pd.Timedelta(seconds=2), periods=5, freq="s"),
            "CPU": [20.0] * 5,
        })
        aligned_dfs, run_duration = core._resample_datasets([df1, df2], ["node1", "node2"], False)
        assert "node1" in aligned_dfs
        assert "node2" in aligned_dfs
        assert run_duration == 6.0

    def test_resample_single_df(self):
        now = pd.Timestamp("2024-01-01")
        df = pd.DataFrame({
            "TIME": pd.date_range(now, periods=5, freq="s"),
            "CPU": [10.0] * 5,
        })
        aligned_dfs, run_duration = core._resample_datasets([df], ["node1"], False)
        assert "node1" in aligned_dfs
        assert run_duration == 4.0


class TestOutlierDetection:
    def test_outlier_detection(self):
        now = pd.Timestamp("2024-01-01")
        times = pd.date_range(now, periods=10, freq="s")
        resampled_dfs = {
            "node1": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node2": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node3": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node4": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node5": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node6": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node7": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node8": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node9": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "outlier1": pd.DataFrame({"CPU": [100.0] * 10}, index=times),
            "outlier2": pd.DataFrame({"CPU": [100.0] * 10}, index=times),
        }
        outliers_dict = core._outlier_detection(resampled_dfs, ["CPU"])
        print(outliers_dict)
        assert "CPU" in outliers_dict
        assert len(outliers_dict["CPU"]) == 2

    def test_outlier_detection_no_outliers(self):
        now = pd.Timestamp("2024-01-01")
        times = pd.date_range(now, periods=10, freq="s")
        resampled_dfs = {
            "node1": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node2": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node3": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
        }
        outliers_dict = core._outlier_detection(resampled_dfs, ["CPU"])
        print(outliers_dict)
        assert "CPU" in outliers_dict
        assert len(outliers_dict["CPU"]) == 0

    def test_outlier_detection_single_node(self):
        now = pd.Timestamp("2024-01-01")
        times = pd.date_range(now, periods=10, freq="s")
        resampled_dfs = {
            "node1": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
        }
        outliers_dict = core._outlier_detection(resampled_dfs, ["CPU"])
        print(outliers_dict)
        assert "CPU" in outliers_dict
        assert len(outliers_dict["CPU"]) == 0

    def test_outlier_detection_missing_column(self):
        now = pd.Timestamp("2024-01-01")
        times = pd.date_range(now, periods=10, freq="s")
        resampled_dfs = {
            "node1": pd.DataFrame({"MEM": [10.0] * 10}, index=times),
            "node2": pd.DataFrame({"MEM": [10.0] * 10}, index=times),
            "node3": pd.DataFrame({"MEM": [10.0] * 10}, index=times),
            "outlier": pd.DataFrame({"MEM": [100.0] * 10}, index=times),
        }
        # Pass BOTH "CPU" (missing from data) and "Memory" (present in data)
        outliers_dict = core._outlier_detection(resampled_dfs, ["CPU", "Memory"])
        
        # CPU should be empty
        assert "CPU" in outliers_dict
        assert len(outliers_dict["CPU"]) == 0
        # "MEM" should be successfully calculated
        assert "MEM" in outliers_dict
        assert len(outliers_dict["MEM"]) == 1

    def test_outlier_detection_wrong_metric(self):
        now = pd.Timestamp("2024-01-01")
        times = pd.date_range(now, periods=10, freq="s")
        resampled_dfs = {
            "node1": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node2": pd.DataFrame({"CPU": [10.0] * 10}, index=times),
            "node3": pd.DataFrame({"CPU": [ 10.0] * 10}, index=times),
            "outlier": pd.DataFrame({"CPU": [100.0] * 10}, index=times),
        }
        # Pass BOTH a garbage string and a valid UI label ("CPU")
        outliers_dict = core._outlier_detection(resampled_dfs, ["FAKE_METRIC_STRING", "CPU"])
        
        # The fake string should be ignored entirely
        assert "FAKE_METRIC_STRING" not in outliers_dict
        assert "GPU_USAGE" not in outliers_dict
        # "CPU" should be successfully calculated
        assert "CPU" in outliers_dict
        assert len(outliers_dict["CPU"]) == 1


class TestIsNodeOutlier:
    def test_is_node_outlier_true(self):
        outliers_dict = {"CPU": [{"node": "node1"}]}
        assert core._is_node_outlier("node1", "CPU", outliers_dict) is True

    def test_is_node_outlier_false(self):
        outliers_dict = {"CPU": [{"node": "node1"}]}
        assert core._is_node_outlier("node2", "CPU", outliers_dict) is False

    def test_is_node_outlier_missing_metric(self):
        outliers_dict = {}
        assert core._is_node_outlier("node1", "CPU", outliers_dict) is False


class TestGetNumberOfPlots:
    def test_all_nodes(self):
        df = pd.DataFrame({"TIME": [], "CPU": [], "MEM": []})
        result = core._get_number_of_plots([df], ["node1"], ["CPU", "Memory"], "All Nodes", {})
        assert result == 2

    def test_outliers_only_no_outliers(self):
        df = pd.DataFrame({"TIME": [], "CPU": [], "MEM": []})
        # For single-node runs, plots are always generated regardless of scope
        result = core._get_number_of_plots([df], ["worker1"], ["CPU", "Memory"], "Outliers Only", {})
        assert result == 2

    def test_single_node_run(self):
        df = pd.DataFrame({"TIME": [], "CPU": [], "MEM": []})
        result = core._get_number_of_plots([df], ["node1"], ["CPU"], "Only Aggregated Plots", {})
        assert result == 1

    def test_master_always_plotted(self):
        df = pd.DataFrame({"TIME": [], "CPU": [], "MEM": []})
        result = core._get_number_of_plots([df, df], ["MASTER", "worker1"], ["CPU"], "Outliers Only", {})
        assert result == 1

    def test_outliers_only_with_outliers(self, tmp_path):
        df1 = pd.DataFrame({
            "TIME": pd.date_range("2024-01-01", periods=5, freq="s"),
            "CPU": [10.0] * 5,
            "MEM": [20.0] * 5,
        })
        df1.to_csv(tmp_path / "stats_node1.csv", index=False)

        df2 = pd.DataFrame({
            "TIME": pd.date_range("2024-01-01", periods=5, freq="s"),
            "CPU": [15.0] * 5,
            "MEM": [25.0] * 5,
        })
        df2.to_csv(tmp_path / "stats_node2.csv", index=False)

        df_list, name_list, max_length = core._load_csv_files(str(tmp_path), False, ["CPU", "Memory"])
        outliers_dict = {"CPU": [{"node": "node1"}]}
        result = core._get_number_of_plots(df_list, ["node1"], ["CPU"], "Outliers Only", outliers_dict)
        assert result == 1


class TestGetPlotOutputPath:
    def test_single_node(self):
        plot, path = core._get_plot_output_path("node1", "/profiling", "All Nodes", True, False, ["CPU"])
        assert plot is True
        assert path == "/profiling"

    def test_master(self):
        plot, path = core._get_plot_output_path("MASTER", "/profiling", "All Nodes", False, True, ["CPU"])
        assert plot is True
        assert path == "/profiling/MASTER"

    def test_all_nodes_worker(self):
        plot, path = core._get_plot_output_path("worker1", "/profiling", "All Nodes", False, False, [])
        assert plot is True
        assert path == "/profiling/other_workers/worker1"

    def test_outliers_only_non_outlier(self):
        plot, path = core._get_plot_output_path("worker1", "/profiling", "Outliers Only", False, False, [])
        assert plot is False
        assert path is None

    def test_cpu_outlier(self):
        plot, path = core._get_plot_output_path("worker1", "/profiling", "Outliers Only", False, False, ["CPU"])
        assert plot is True
        assert path == "/profiling/outliers_cpu/worker1"

    def test_mem_outlier(self):
        plot, path = core._get_plot_output_path("worker1", "/profiling", "Outliers Only", False, False, ["Memory"])
        assert plot is True
        assert path == "/profiling/outliers_memory/worker1"

    def test_cpu_and_mem_outlier(self):
        plot, path = core._get_plot_output_path("worker1", "/profiling", "Outliers Only", False, False, ["CPU", "Memory"])
        assert plot is True
        assert path == "/profiling/outliers_cpu_memory/worker1"

    def test_gpu_outlier(self):
        plot, path = core._get_plot_output_path("worker1", "/profiling", "Outliers Only", False, False, ["GPU"])
        assert plot is True
        assert path == "/profiling/outliers_gpu/worker1"

    def test_gpu_mem_outlier(self):
        plot, path = core._get_plot_output_path("worker1", "/profiling", "Outliers Only", False, False, ["GPU Memory"])
        assert plot is True
        assert path == "/profiling/outliers_gpu_memory/worker1"
        assert plot is True
        assert path == "/profiling/outliers_gpu_memory/worker1"


class TestExecuteGeneration:
    def test_execute_only_stats(self, tmp_path):
        df = pd.DataFrame({
            "TIME": pd.date_range("2024-01-01", periods=5, freq="s"),
            "CPU": [10.0] * 5,
            "MEM": [20.0] * 5,
        })
        df.to_csv(tmp_path / "stats_node1.csv", index=False)

        config = {
            "data_dir": str(tmp_path),
            "selected_metrics": ["CPU", "Memory"],
            "plot_scope": "Only Stats Analysis",
            "format": "svg",
            "print_message": False,
            "print_stats": False,
            "output_plots_dir": str(tmp_path),
        }
        result = core.execute_generation(config)
        assert result is None

    def test_execute_no_valid_csvs(self, tmp_path):
        config = {
            "data_dir": str(tmp_path),
            "selected_metrics": ["CPU"],
            "plot_scope": "All Nodes",
            "format": "svg",
            "print_message": False,
            "print_stats": False,
            "output_plots_dir": str(tmp_path),
        }
        # Patch `core.console` to suppress console output and capture calls
        with patch.object(core, "console") as mock_console:
            # Expect SystemExit because there are no valid CSV files in `config["data_dir"]`
            with pytest.raises(SystemExit) as exc_info:
                # Execute the generation logic which should exit with code 1
                core.execute_generation(config)
            # Verify the exit code is 1
            assert exc_info.value.code == 1

    def test_execute_with_plots(self, tmp_path):
        df = pd.DataFrame({
            "TIME": pd.date_range("2024-01-01", periods=5, freq="s"),
            "CPU": [10.0] * 5,
            "MEM": [20.0] * 5,
        })
        df.to_csv(tmp_path / "stats_node1.csv", index=False)

        config = {
            "data_dir": str(tmp_path),
            "selected_metrics": ["CPU", "Memory"],
            "plot_scope": "All Nodes",
            "format": "svg",
            "print_message": False,
            "print_stats": False,
            "output_plots_dir": str(tmp_path),
        }
        with patch.object(core, "_run_plotting_with_progress"):
            result = core.execute_generation(config)
            assert result == str(tmp_path)


class TestParseCliArgs:
    def test_parse_defaults(self):
        with patch.object(sys, "argv", ["prog", "--dir=/data"]):
            args = core._parse_cli_args()
            assert args.data_dir == "/data"
            assert args.silent is False
            assert args.format == "svg"
            assert args.metrics == list(DEFAULT_METRICS)

    def test_parse_silent(self):
        with patch.object(sys, "argv", ["prog", "--dir=/data", "--silent"]):
            args = core._parse_cli_args()
            assert args.silent is True
            assert args.data_dir == "/data"
            assert args.format == "svg"
            assert args.metrics == list(DEFAULT_METRICS)

    def test_parse_metrics(self):
        with patch.object(sys, "argv", ["prog", "--dir=/data", "--metrics=cpu,mem"]):
            args = core._parse_cli_args()
            assert args.data_dir == "/data"
            assert args.silent is False
            assert args.format == "svg"
            assert args.metrics == ["cpu", "mem"]

    def test_parse_invalid_metrics(self):
        with patch.object(sys, "argv", ["prog", "--dir=/data", "--metrics=invalid"]):
            with pytest.raises(SystemExit):
                core._parse_cli_args()

    def test_parse_format(self):
        with patch.object(sys, "argv", ["prog", "--dir=/data", "--format=png"]):
            args = core._parse_cli_args()
            assert args.data_dir == "/data"
            assert args.silent is False
            assert args.format == "png"
            assert args.metrics == list(DEFAULT_METRICS)
    
    def test_parse_silent_mode(self):
        with patch.object(sys, "argv", ["prog", "--dir=/data", "--silent"]):
            args = core._parse_cli_args()
            assert args.silent is True
            assert args.data_dir == "/data"
            assert args.format == "svg"
            assert args.metrics == list(DEFAULT_METRICS)
    
    def test_parse_output_dir(self):
        with patch.object(sys, "argv", ["prog", "--dir=/data", "--output_dir=/output"]):
            args = core._parse_cli_args()
            assert args.data_dir == "/data"
            assert args.output_dir == "/output"
            assert args.silent is False
            assert args.format == "svg"
            assert args.metrics == list(DEFAULT_METRICS)

class TestResolveScope:
    def test_resolve_scope_all_nodes(self):
        args = argparse.Namespace()
        assert core._resolve_scope(args) == "All Nodes"

    def test_resolve_scope_outliers(self):
        args = argparse.Namespace(outliers=True)
        assert core._resolve_scope(args) == "Outliers Only"


class TestResolveMetrics:
    def test_resolve_metrics_defaults(self):
        args = argparse.Namespace()
        assert core._resolve_metrics(args) == list(DEFAULT_METRICS)

    def test_resolve_metrics_selected(self):
        args = argparse.Namespace(metrics=["cpu", "mem"])
        assert core._resolve_metrics(args) == ["CPU", "Memory"]


class TestGetHeadlessConfig:
    def test_valid_directory(self):
        args = argparse.Namespace(data_dir="/tmp", format="svg")
        with patch("os.path.isdir", return_value=True):
            config = core._get_headless_config(args)
            assert config["data_dir"] == "/tmp"
            assert config["format"] == "svg"
            assert config["print_stats"] is False
            assert config["print_message"] is False
            assert config["plot_scope"] == "All Nodes"
            assert config["output_plots_dir"] == "/tmp"
    
    def test_autodetected_stats_directory(self):
        args = argparse.Namespace(data_dir=core.DEFAULT_STATS_SUBDIR, format="svg")
        with patch("os.path.isdir", return_value=True):
            config = core._get_headless_config(args)
            assert config["data_dir"] == core.DEFAULT_STATS_SUBDIR
            assert config["output_plots_dir"] == core.DEFAULT_PLOTS_DIR

    def test_invalid_directory(self):
        args = argparse.Namespace(data_dir="/nonexistent", format="svg")
        with patch("os.path.isdir", return_value=False):
            with pytest.raises(IsADirectoryError):
                core._get_headless_config(args)


# Tests on generate indivudal plots
class TestGenerateIndividualPlots:
    def test_generate_individual_plots(self, tmp_path):
        df = pd.DataFrame({
            "TIME": pd.date_range("2024-01-01", periods=5, freq="s"),
            "CPU": [10.0] * 5,
            "MEM": [20.0] * 5,
        })
        df.to_csv(tmp_path / "stats_node1.csv", index=False)

        config = {
            "data_dir": str(tmp_path),
            "selected_metrics": ["CPU", "Memory"],
            "plot_scope": "All Nodes",
            "format": "svg",
            "print_message": False,
            "print_stats": False,
            "output_plots_dir": str(tmp_path),
        }
        with patch.object(core, "_run_plotting_with_progress") as mock_plotting:
            result = core.execute_generation(config)
            assert result == str(tmp_path)
            mock_plotting.assert_called_once()
