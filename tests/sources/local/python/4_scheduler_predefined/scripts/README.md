# Scripts Directory

This directory contains Python scripts used for validating the Predefined Scheduler test.

## Scripts

### `generate_config.py`

**Purpose**: Extract actual task scheduling information from `runtime.log` and generate a `config.json` file.

**Usage**:
```bash
python3 generate_config.py runtime.log > actual_config.json
```

**Status**: ⚠️ **PLACEHOLDER** - You need to replace this with your actual implementation!

**What it should do**:
1. Parse the `runtime.log` file
2. Extract for each task:
   - `taskId`: The task identifier
   - `implementationId`: Which implementation was used
   - `predecessors`: List of predecessor task IDs
   - `resource`: Which worker executed the task
3. Output a JSON array in the same format as `config.json`

**Example output**:
```json
[
  {
    "taskId": 1,
    "implementationId": 0,
    "predecessors": [],
    "resource": "COMPSsWorker01"
  },
  ...
]
```

**How to implement**:
1. Copy your existing `generate_config.py` script here
2. Make sure it accepts `runtime.log` as the first argument
3. Ensure it outputs valid JSON to stdout
4. Test it manually: `python3 generate_config.py <path-to-runtime.log>`

---

### `compare_configs.py`

**Purpose**: Compare expected and actual config files and report differences.

**Usage**:
```bash
python3 compare_configs.py expected.json actual.json
```

**Status**: ✅ **IMPLEMENTED** - Ready to use!

**What it does**:
1. Loads both JSON files
2. Compares each task's:
   - `taskId` (checks for missing/extra tasks)
   - `implementationId`
   - `resource`
   - `predecessors` (order-independent comparison)
3. Reports all differences found
4. Exits with code 0 if configs match, 1 if they differ

**Example output**:
```
✓ Configurations match perfectly!
  - 10 tasks validated
```

Or if there are differences:
```
✗ Configurations differ:
  - Task 5: resource mismatch - expected 'COMPSsWorker01', got 'COMPSsWorker02'
  - Task 7: predecessors mismatch - expected [6], got [5, 6]
```

---

## Integration with Test

The `result` script automatically uses these scripts if they are present:

1. **Extracts actual config**: Runs `generate_config.py` on `runtime.log`
2. **Compares configs**: Runs `compare_configs.py` to validate
3. **Reports results**: Shows whether the scheduler followed the plan
4. **Fails test if mismatch**: Returns exit code 1 if configs don't match

If the scripts are not found or `generate_config.py` is not implemented, the test will skip the detailed comparison and show a warning.

---

## Quick Test

To test your `generate_config.py` implementation:

```bash
# 1. Run the test to generate a runtime.log
cd ..
./run_manual_test.sh

# 2. Find the runtime.log
RUNTIME_LOG=$(find /tmp/compss_test_predefined_* -name "runtime.log" | head -1)

# 3. Test generate_config.py
python3 scripts/generate_config.py "$RUNTIME_LOG" > /tmp/test_config.json

# 4. Validate the output
cat /tmp/test_config.json

# 5. Compare with expected
python3 scripts/compare_configs.py config.json /tmp/test_config.json
```

---

## Notes

- Both scripts should be executable: `chmod +x *.py`
- They should work with Python 3.6+
- No external dependencies required (only standard library)
- Error messages should go to stderr, JSON output to stdout
