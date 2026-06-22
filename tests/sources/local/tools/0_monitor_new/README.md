# New Monitor Event Export Test

This test validates the new monitor event export path for a small PyCOMPSs
application. It does not validate the full runtime output nor the temporal OTLP
metrics stream. The test focuses on the JSON events exported by the runtime to:

- `/monitored-events`
- `/graph-events`

## What The Test Runs

The application in `src/test_monitor_new.py` launches a minimal workflow with
three chained tasks:

- `test_monitor_new.task_one`
- `test_monitor_new.task_two`
- `test_monitor_new.task_three`

The execution script enables the monitor tracing backend with:

```bash
--env_script="${base_app_dir}/src/monitor_env.sh"
--tracing=monitor
```

The test also starts a local HTTP capture server before launching COMPSs. The
server listens on a free dynamic port and the execution passes explicit JVM
properties for `compss.events.api` and `compss.graph.api` so the test does not
depend on port `8088` being free or on an external monitor service running.

## Captured Files

For a normal tests.py execution, the harness creates a folder like:

```text
~/tests_execution_sandbox/logs/app<id>_<retry>/
```

For this test, the captured monitor payloads are stored under:

```text
test_monitor_new.py_python3_0<retry>_capture/monitored_events.jsonl
test_monitor_new.py_python3_0<retry>_capture/graph_events.jsonl
```

Each line is one JSON object received by the local capture server.

## What Is Validated

The result script first checks that the application finished correctly and that
COMPSs did not report fatal runtime errors. Then `src/monitor_capture.py`
validates the captured monitor data.

For `monitored_events.jsonl`, the test checks that:

- events were received;
- every event contains the expected common fields: `ts`, `run_id`, `agent_id`,
  `node_name`, `thread_type`, `thread_id`, `event_type`, `event_code`, and
  `event_name`;
- `run_id` and `thread_type` are populated;
- both `local-master` and `local-worker` agents emitted events;
- master events use `node_name == "master"`;
- worker events use a local worker node name (`localhost` or `COMPSsWorker01`,
  depending on the runtime/resource naming path);
- the expected thread types `AP`, `TD`, and `EXEC` appear;
- representative event names are present, including:
  - `Task Dispatcher: Execute tasks`
  - `Access Processor: Barrier`
  - `task_one`
- task registry events (`event_type == 88000000`) are emitted for the three
  application core elements:
  - `task_one`
  - `task_two`
  - `task_three`
- each registry signature maps consistently to exactly one core id
  (`event_code`);
- the three application signatures map to three distinct core ids. This catches
  regressions where the monitor exports an implementation id or a constant
  value instead of the real core element id.

For `graph_events.jsonl`, the test checks that:

- graph events were received;
- every graph event contains `ts`, `run_id`, `app_id`, `type`, and
  `master_name`;
- `master_name` is `local-master`;
- representative graph event types are present:
  - `APP_START`
  - `TASK_CREATED`
  - `DATA_DEP`
  - `TASK_FINISHED`
  - `APP_END`
- at least one task creation payload matches:

```json
{"task_id": 1, "task_name": "test_monitor_new.task_one"}
```

- the set of created task names is exactly the three application tasks;
- the set of finished task names matches the created tasks and uses the same
  task ids;
- data dependencies describe the application chain:
  - `test_monitor_new.task_one -> test_monitor_new.task_two`
  - `test_monitor_new.task_two -> test_monitor_new.task_three`

Finally, the test checks that monitored events and graph events share a common
`run_id`.

## What Is Intentionally Not Validated

The test does not require a strict event order and does not compare the complete
event stream against a golden output. The runtime can emit extra events without
failing this test.

The test also does not validate OTLP metrics. Those metrics are temporal and are
exported through the OpenTelemetry path, which is outside the scope of this
test.
