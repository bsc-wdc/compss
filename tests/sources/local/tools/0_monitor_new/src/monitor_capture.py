#!/usr/bin/env python3

import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


TASK_REGISTRY_EVENT_TYPE = 88000000
EXPECTED_REGISTRY_SIGNATURES = {"task_one", "task_two", "task_three"}
EXPECTED_TASK_NAMES = {
    "test_monitor_new.task_one",
    "test_monitor_new.task_two",
    "test_monitor_new.task_three",
}
EXPECTED_WORKER_NODE_NAMES = {"localhost", "COMPSsWorker01"}
EXPECTED_DATA_DEPENDENCIES = {
    ("test_monitor_new.task_one", "test_monitor_new.task_two"),
    ("test_monitor_new.task_two", "test_monitor_new.task_three"),
}


def _append_jsonl(path, item):
    with open(path, "a", encoding="utf-8") as handler:
        handler.write(json.dumps(item, sort_keys=True))
        handler.write("\n")
        handler.flush()


def serve(args):
    events_file = os.path.abspath(args.events_file)
    graph_file = os.path.abspath(args.graph_file)
    port_file = os.path.abspath(args.port_file)

    for file_path in (events_file, graph_file):
        open(file_path, "w", encoding="utf-8").close()

    class CaptureHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(length).decode("utf-8")
            try:
                decoded = json.loads(raw_body) if raw_body else []
            except json.JSONDecodeError as exc:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(str(exc).encode("utf-8"))
                return

            items = decoded if isinstance(decoded, list) else [decoded]
            if self.path == "/monitored-events":
                output = events_file
            elif self.path == "/graph-events":
                output = graph_file
            else:
                output = None

            if output is not None:
                for item in items:
                    _append_jsonl(output, item)

            self.send_response(204)
            self.end_headers()

        def log_message(self, fmt, *args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), CaptureHandler)
    with open(port_file, "w", encoding="utf-8") as handler:
        handler.write(str(server.server_port))
        handler.write("\n")
    server.serve_forever()


def _load_jsonl(path):
    if not os.path.exists(path):
        raise AssertionError("Missing capture file: {}".format(path))

    items = []
    with open(path, "r", encoding="utf-8") as handler:
        for line in handler:
            line = line.strip()
            if line:
                items.append(json.loads(line))
    return items


def _require(condition, message):
    if not condition:
        raise AssertionError(message)


def _validate_monitored_events(events):
    _require(events, "No monitored events were captured")

    required = {
        "ts",
        "run_id",
        "agent_id",
        "node_name",
        "thread_type",
        "thread_id",
        "event_type",
        "event_code",
        "event_name",
    }
    for event in events:
        missing = required.difference(event)
        _require(not missing, "Monitored event missing fields: {}".format(sorted(missing)))
        _require(event["run_id"], "Monitored event has empty run_id")
        _require(event["thread_type"], "Monitored event has empty thread_type")

    agents = {event["agent_id"] for event in events}
    _require("local-master" in agents, "Missing local-master monitored events")
    _require("local-worker" in agents, "Missing local-worker monitored events")

    master_events = [event for event in events if event["agent_id"] == "local-master"]
    worker_events = [event for event in events if event["agent_id"] == "local-worker"]
    _require(any(event["node_name"] == "master" for event in master_events), "local-master node_name is not master")
    worker_node_names = {event["node_name"] for event in worker_events}
    _require(
        worker_node_names.issubset(EXPECTED_WORKER_NODE_NAMES),
        "Unexpected local-worker node_name values: {}".format(sorted(worker_node_names)),
    )

    thread_types = {event["thread_type"] for event in events}
    _require({"AP", "TD", "EXEC"}.issubset(thread_types), "Missing AP, TD or EXEC monitored thread type")

    event_names = {event["event_name"] for event in events}
    _require("Task Dispatcher: Execute tasks" in event_names, "Missing task dispatcher execute event")
    _require("Access Processor: Barrier" in event_names, "Missing access processor barrier event")
    _require("task_one" in event_names, "Missing task_one execution event")

    _validate_task_registry_events(events)


def _validate_task_registry_events(events):
    registry_events = [event for event in events if event["event_type"] == TASK_REGISTRY_EVENT_TYPE]
    _require(registry_events, "No task registry events were captured")

    for event in registry_events:
        _require(event["agent_id"] == "local-master", "Task registry event was not emitted by local-master")
        _require(event["node_name"] == "master", "Task registry event node_name is not master")
        _require(event["thread_type"] == "REGISTRY", "Task registry event thread_type is not REGISTRY")
        _require(isinstance(event["event_code"], int), "Task registry event_code is not an integer")
        _require(event["event_code"] > 0, "Task registry event_code is not a positive core id")

    signature_to_core_ids = {}
    for event in registry_events:
        signature = event["event_name"]
        if signature in EXPECTED_REGISTRY_SIGNATURES:
            signature_to_core_ids.setdefault(signature, set()).add(event["event_code"])

    missing = EXPECTED_REGISTRY_SIGNATURES.difference(signature_to_core_ids)
    _require(not missing, "Missing task registry signatures: {}".format(sorted(missing)))

    unstable = {
        signature: sorted(core_ids)
        for signature, core_ids in signature_to_core_ids.items()
        if len(core_ids) != 1
    }
    _require(not unstable, "Task registry signatures map to multiple core ids: {}".format(unstable))

    core_ids = {next(iter(core_ids)) for core_ids in signature_to_core_ids.values()}
    _require(
        len(core_ids) == len(EXPECTED_REGISTRY_SIGNATURES),
        "Task registry signatures do not map to distinct core ids: {}".format(signature_to_core_ids),
    )


def _validate_graph_events(graph_events):
    _require(graph_events, "No graph events were captured")

    required = {"ts", "run_id", "app_id", "type", "master_name"}
    for event in graph_events:
        missing = required.difference(event)
        _require(not missing, "Graph event missing fields: {}".format(sorted(missing)))
        _require(event["run_id"], "Graph event has empty run_id")
        _require(event["master_name"] == "local-master", "Unexpected graph master_name: {}".format(event["master_name"]))

    event_types = {event["type"] for event in graph_events}
    expected_types = {"APP_START", "TASK_CREATED", "DATA_DEP", "TASK_FINISHED", "APP_END"}
    _require(expected_types.issubset(event_types), "Missing graph event types: {}".format(sorted(expected_types - event_types)))

    expected_payload = {"task_id": 1, "task_name": "test_monitor_new.task_one"}
    _require(
        any(event.get("type") == "TASK_CREATED" and event.get("payload") == expected_payload for event in graph_events),
        "Missing expected task_one TASK_CREATED payload",
    )

    _validate_application_graph_events(graph_events)


def _payload(event):
    payload = event.get("payload")
    _require(payload is None or isinstance(payload, dict), "Graph event payload is not a JSON object")
    return payload or {}


def _validate_application_graph_events(graph_events):
    task_created = [event for event in graph_events if event["type"] == "TASK_CREATED"]
    task_finished = [event for event in graph_events if event["type"] == "TASK_FINISHED"]
    data_deps = [event for event in graph_events if event["type"] == "DATA_DEP"]

    created_by_id = {}
    for event in task_created:
        payload = _payload(event)
        task_id = payload.get("task_id")
        task_name = payload.get("task_name")
        _require(isinstance(task_id, int), "TASK_CREATED payload task_id is not an integer")
        _require(task_name in EXPECTED_TASK_NAMES, "Unexpected TASK_CREATED task_name: {}".format(task_name))
        _require(task_id not in created_by_id, "Duplicated TASK_CREATED task_id: {}".format(task_id))
        created_by_id[task_id] = task_name

    _require(set(created_by_id.values()) == EXPECTED_TASK_NAMES, "Unexpected TASK_CREATED task set")

    finished_by_id = {}
    for event in task_finished:
        payload = _payload(event)
        task_id = payload.get("task_id")
        task_name = payload.get("task_name")
        _require(isinstance(task_id, int), "TASK_FINISHED payload task_id is not an integer")
        _require(task_name in EXPECTED_TASK_NAMES, "Unexpected TASK_FINISHED task_name: {}".format(task_name))
        _require(task_id not in finished_by_id, "Duplicated TASK_FINISHED task_id: {}".format(task_id))
        finished_by_id[task_id] = task_name

    _require(finished_by_id == created_by_id, "TASK_FINISHED ids do not match TASK_CREATED ids")

    dependency_pairs = set()
    for event in data_deps:
        payload = _payload(event)
        producer_name = payload.get("producer_name")
        consumer_name = payload.get("consumer_name")
        producer_id = payload.get("producer_id")
        consumer_id = payload.get("consumer_id")
        _require(producer_name in EXPECTED_TASK_NAMES, "Unexpected DATA_DEP producer_name: {}".format(producer_name))
        _require(consumer_name in EXPECTED_TASK_NAMES, "Unexpected DATA_DEP consumer_name: {}".format(consumer_name))
        _require(created_by_id.get(producer_id) == producer_name, "DATA_DEP producer_id does not match producer_name")
        _require(created_by_id.get(consumer_id) == consumer_name, "DATA_DEP consumer_id does not match consumer_name")
        dependency_pairs.add((producer_name, consumer_name))

    _require(dependency_pairs == EXPECTED_DATA_DEPENDENCIES, "Unexpected DATA_DEP graph: {}".format(dependency_pairs))


def validate(args):
    events = _load_jsonl(args.events_file)
    graph_events = _load_jsonl(args.graph_file)

    _validate_monitored_events(events)
    _validate_graph_events(graph_events)

    event_run_ids = {event["run_id"] for event in events if event.get("run_id")}
    graph_run_ids = {event["run_id"] for event in graph_events if event.get("run_id")}
    _require(event_run_ids.intersection(graph_run_ids), "Monitored and graph events do not share a run_id")

    print("Captured monitored events:", len(events))
    print("Captured graph events:", len(graph_events))


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--events-file", required=True)
    serve_parser.add_argument("--graph-file", required=True)
    serve_parser.add_argument("--port-file", required=True)
    serve_parser.set_defaults(func=serve)

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--events-file", required=True)
    validate_parser.add_argument("--graph-file", required=True)
    validate_parser.set_defaults(func=validate)

    args = parser.parse_args()
    try:
        args.func(args)
    except AssertionError as exc:
        print("ERROR:", exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
