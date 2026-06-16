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
"""Unit tests for src/stats.py."""

from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import pandas as pd
from pandas import Timestamp
import pytest

from src import stats


class TestMergeShortStages:
    def test_empty_list(self):
        result = stats._merge_short_stages([], 100.0)
        assert result == []

    def test_single_stage(self):
        now = datetime.now()
        raw = [("Peak Compute", now, now + timedelta(seconds=50), 80.0, 100.0, 60.0)]
        result = stats._merge_short_stages(raw, 100.0)
        assert len(result) == 1
        assert result[0][0] == "Peak Compute"

    def test_merge_adjacent_same_name(self):
        now = datetime.now()
        raw = [
            ("Peak Compute", now, now + timedelta(seconds=10), 80.0, 100.0, 60.0),
            ("Peak Compute", now + timedelta(seconds=10), now + timedelta(seconds=20), 85.0, 95.0, 70.0),
        ]
        result = stats._merge_short_stages(raw, 100.0)
        assert len(result) == 1
        assert result[0][0] == "Peak Compute"

    def test_merge_small_stage_into_neighbor(self):
        now = datetime.now()
        raw = [
            ("Peak Compute", now, now + timedelta(seconds=80), 80.0, 100.0, 60.0),
            ("Idle", now + timedelta(seconds=80), now + timedelta(seconds=83), 5.0, 10.0, 0.0),
            ("Ramping", now + timedelta(seconds=83), now + timedelta(seconds=100), 40.0, 50.0, 30.0),
        ]
        result = stats._merge_short_stages(raw, 100.0)
        assert len(result) == 2

    def test_merge_several_short_stages(self):
        raw = [
            ('Ramping / Moderate Load', Timestamp('2026-06-06 12:30:10'), Timestamp('2026-06-06 12:30:15'), 4.79, 4.79, 4.79),
            ('Idle / I/O Wait', Timestamp('2026-06-06 12:30:15'), Timestamp('2026-06-06 12:30:45'), 0.82, 2.02, 0.09),
            ('Ramping / Moderate Load', Timestamp('2026-06-06 12:30:45'), Timestamp('2026-06-06 12:33:33'), 3.88, 5.12, 2.62),
            ('Idle / I/O Wait', Timestamp('2026-06-06 12:33:33'), Timestamp('2026-06-06 12:33:34'), 3.91, 3.91, 3.91),
            ('Ramping / Moderate Load', Timestamp('2026-06-06 12:33:34'), Timestamp('2026-06-06 12:33:38'), 2.59, 2.59, 2.59),
            ('Idle / I/O Wait', Timestamp('2026-06-06 12:33:38'), Timestamp('2026-06-06 12:33:44'), 1.17, 2.24, 0.10),
            ('Underperforming / Tailing Off', Timestamp('2026-06-06 12:33:44'), Timestamp('2026-06-06 12:33:59'), 6.76, 8.43, 5.46),
            ('Ramping / Moderate Load', Timestamp('2026-06-06 12:33:59'), Timestamp('2026-06-06 12:34:04'), 3.61, 3.61, 3.61),
            ('Underperforming / Tailing Off', Timestamp('2026-06-06 12:34:04'), Timestamp('2026-06-06 12:34:14'), 15.77, 23.71, 8.36),
            ('Idle / I/O Wait', Timestamp('2026-06-06 12:34:14'), Timestamp('2026-06-06 12:34:14'), 0.40, 0.40, 0.40),
        ]
        run_dur = raw[-1][2] - raw[0][1]
        result = stats._merge_short_stages(raw, run_dur.total_seconds())
        print(result)
        assert len(result) == 3


class TestIsMasterOrchestrator:
    def test_master_low_cpu(self):
        df = pd.DataFrame({"CPU": [1.0, 2.0, 1.5]})
        assert stats._is_master_orchestrator("MASTER_node", df) == True

    def test_master_high_cpu(self):
        df = pd.DataFrame({"CPU": [10.0, 20.0, 15.0]})
        assert stats._is_master_orchestrator("MASTER_node", df) == False

    def test_worker_node(self):
        df = pd.DataFrame({"CPU": [1.0, 2.0]})
        assert stats._is_master_orchestrator("worker1", df) is False


class TestNormalizeResampledDfs:
    def test_none_input(self):
        assert stats._normalize_resampled_dfs(None) == {}

    def test_empty_dict(self):
        assert stats._normalize_resampled_dfs({}) == {}

    def test_already_dict(self):
        data = {"node1": pd.DataFrame({"CPU": [1, 2, 3]})}
        result = stats._normalize_resampled_dfs(data)
        assert result == data

    def test_dataframe_input_with_metric(self):
        df = pd.DataFrame({"CPU": [1, 2, 3], "MEM": [4, 5, 6]})
        result = stats._normalize_resampled_dfs(df, metric="CPU")
        assert "node" in result
        assert "CPU" in result["node"].columns

    def test_series_input(self):
        s = pd.Series([1, 2, 3], name="CPU")
        result = stats._normalize_resampled_dfs(s, metric="CPU")
        assert "CPU" in result
        assert "CPU" in result["CPU"].columns


class TestIdentifyTimeSeriesOutliers:
    def test_empty_input(self):
        result = stats.identify_time_series_outliers({}, "CPU")
        assert result == []

    def test_no_workers_with_metric(self):
        data = {"node1": pd.DataFrame({"MEM": [1, 2, 3]})}
        result = stats.identify_time_series_outliers(data, "CPU")
        assert result == []

    def test_all_nodes_similar(self):
        data = {
            "node1": pd.DataFrame({"CPU": [10.0] * 10}),
            "node2": pd.DataFrame({"CPU": [11.0] * 10}),
            "node3": pd.DataFrame({"CPU": [10.0] * 10}),
            "node4": pd.DataFrame({"CPU": [11.0] * 10}),
            "node5": pd.DataFrame({"CPU": [10.0] * 10}),
        }
        outliers = stats.identify_time_series_outliers(data, "CPU")
        assert len(outliers) == 0
        assert outliers == []

    def test_detects_outlier(self):
        data = {
            "node1": pd.DataFrame({"CPU": [10.0] * 10}),
            "node2": pd.DataFrame({"CPU": [10.0] * 10}),
            "node3": pd.DataFrame({"CPU": [10.0] * 10}),
            "node4": pd.DataFrame({"CPU": [10.0] * 10}),
            "node5": pd.DataFrame({"CPU": [10.0] * 10}),
            "node6": pd.DataFrame({"CPU": [10.0] * 10}),
            "node7": pd.DataFrame({"CPU": [10.0] * 10}),
            "node8": pd.DataFrame({"CPU": [10.0] * 10}),
            "node9": pd.DataFrame({"CPU": [10.0] * 10}),
            "outlier": pd.DataFrame({"CPU": [100.0] * 10}),
        }
        outliers = stats.identify_time_series_outliers(data, "CPU")
        assert len(outliers) == 1
        assert outliers[0]["node"] == "outlier"
        assert outliers[0]["total"] == 1000.0
        assert isinstance(outliers[0], dict)

    def test_small_cluster_fallback(self):
        data = {
            "node1": pd.DataFrame({"CPU": [10.0] * 10}),
            "node2": pd.DataFrame({"CPU": [10.0] * 10}),
            "node3": pd.DataFrame({"CPU": [100.0] * 10}),
        }
        outliers = stats.identify_time_series_outliers(data, "CPU")
        assert len(outliers) == 1
        assert outliers[0]["node"] == "node3"
        assert outliers[0]["total"] == 1000.0
        assert isinstance(outliers[0], dict)

    def test_master_excluded_from_baseline(self):
        data = {
            "MASTER": pd.DataFrame({"CPU": [1.0] * 10}),
            "node1": pd.DataFrame({"CPU": [10.0] * 10}),
            "node2": pd.DataFrame({"CPU": [10.0] * 10}),
            "node3": pd.DataFrame({"CPU": [10.0] * 10}),
            "node4": pd.DataFrame({"CPU": [10.0] * 10}),
            "node5": pd.DataFrame({"CPU": [10.0] * 10}),
        }
        outliers = stats.identify_time_series_outliers(data, "CPU")
        assert len(outliers) == 0
        assert outliers == []


class TestAppendResourceStageWarning:
    def test_no_warnings_if_below_duration_threshold(self):
        warnings = []
        # 10.0s is below the 15.0s threshold
        stats._append_resource_stage_warning("CPU", 100.0, 5.0, 10.0, 10.0, warnings)
        assert len(warnings) == 0

    def test_no_warnings_if_below_percentage_threshold(self):
        warnings = []
        # 20.0s is 2% of 1000.0s, which is below the 5.0% threshold
        stats._append_resource_stage_warning("CPU", 1000.0, 5.0, 20.0, 20.0, warnings)
        assert len(warnings) == 0

    def test_append_cpu_underperforming_warning(self):
        warnings = []
        # 20.0s is 20% of 100.0s, above both 15.0s and 5.0%
        stats._append_resource_stage_warning("CPU", 100.0, 5.0, 20.0, 0.0, warnings)
        assert len(warnings) == 1
        assert warnings[0].__class__.__name__ == "Panel"
        assert "Notice: Periods of lower utilization" in str(warnings[0].renderable)

    def test_append_gpu_hanging_warning(self):
        warnings = []
        stats._append_resource_stage_warning("GPU", 100.0, 5.0, 0.0, 20.0, warnings)
        assert len(warnings) == 1
        assert warnings[0].__class__.__name__ == "Panel"
        assert "Prolonged GPU Inactivity Detected" in str(warnings[0].renderable)
