# Predefined Scheduler Test

## Description

This test validates the **Predefined Scheduler** functionality in COMPSs. The Predefined Scheduler allows users to specify a predetermined execution plan for tasks through a JSON configuration file.

## Test Structure

### Files

- **`src/test_predefined.py`**: Python application that creates 10 simple tasks
- **`config.json`**: Predefined scheduler configuration defining:
  - Task execution order
  - Task dependencies (predecessors)
  - Resource assignments for each task
  - Implementation IDs
- **`scripts/generate_config.py`**: Extracts actual task scheduling from runtime.log
- **`scripts/compare_configs.py`**: Compares expected vs actual configuration
- **`project.xml`**: Project configuration with 3 worker nodes
- **`resources.xml`**: Resources configuration for the workers
- **`execution`**: Script to run the test with the Predefined Scheduler
- **`result`**: Validation script that:
  - Checks no errors occurred during execution
  - Verifies the Predefined Scheduler was used
  - Validates all tasks were scheduled
  - Extracts actual config from runtime.log using `generate_config.py`
  - Compares it with expected `config.json` using `compare_configs.py`
  - **Fails if the scheduler didn't follow the predefined plan**

## Configuration Example

The `config.json` defines a task graph with dependencies:

```json
{
  "taskId": 1,
  "implementationId": 0,
  "predecessors": [],
  "resource": "COMPSsWorker01"
}
```

Each task specifies:
- **taskId**: Unique identifier for the task
- **implementationId**: Which implementation to use (for tasks with multiple implementations)
- **predecessors**: Array of task IDs that must complete before this task
- **resource**: Which worker node should execute this task (or **resources** array for multi-node tasks)

## What the Test Validates

1. **Scheduler Loading**: Verifies the Predefined Scheduler is correctly loaded
2. **Task Scheduling**: Ensures all 10 tasks are scheduled
3. **Configuration Parsing**: Validates the config.json is properly read
4. **Resource Assignment**: Checks that all 3 workers are utilized
5. **Exact Config Match**: Extracts actual scheduling from runtime.log and compares with expected config.json
6. **No Crashes**: Ensures the execution completes without errors

## Validation Scripts

### `generate_config.py`

Parses `runtime.log` and extracts the actual task scheduling information, generating a JSON file in the same format as `config.json`.

**Usage**: `python3 generate_config.py runtime.log > actual_config.json`

### `compare_configs.py`

Compares two config files and reports any differences in:
- Task IDs (missing/extra tasks)
- Implementation IDs
- Resource assignments
- Predecessors

**Usage**: `python3 compare_configs.py expected.json actual.json`

The test automatically uses these scripts to validate that the scheduler followed the predefined plan exactly.

## Expected Behavior

- All 10 tasks should execute successfully
- Tasks should respect the dependency order defined in config.json
- Tasks should be assigned to the resources specified in config.json
- No errors should appear in the logs
- The runtime.log should contain evidence of the Predefined Scheduler being used
- The actual scheduling (extracted from runtime.log) should match the expected config.json exactly
