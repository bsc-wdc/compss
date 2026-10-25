# AGENTS.md

This file provides guidance to AI coding agents (Claude Code, opencode, Cursor, Codex, etc.) when working with code in this repository. Read it before making changes.

## What is COMPSs

COMPSs (COMP Superscalar) is a distributed computing framework by the Barcelona Supercomputing Center (BSC). It automatically parallelizes applications written in Java, Python (PyCOMPSs), C/C++, and R (RCOMPSs) across distributed infrastructures (clusters, grids, clouds) by building a task dependency graph at runtime.

## Build

**Prerequisites:** `JAVA_HOME` must be set, git submodules initialized, and all system dependencies installed.

```bash
# Initialize submodules (first time)
./submodules_get.sh

# Build and install to a local directory
cd builders/
INSTALL_DIR=$HOME/opt/COMPSs/
./buildlocal [options] ${INSTALL_DIR}
```

Common `buildlocal` flags (all default to enabled except `--rcompss`):
- `--no-tests` — skip test compilation (faster builds)
- `--no-bindings` / `--no-pycompss` / `--no-rcompss` — disable language bindings
- `--no-tracing` — disable Extrae tracing integration
- `--no-monitor` — disable the web-based monitoring tool
- `--no-python-style` — skip Python style checks

The build uses Maven internally. To run Maven directly on specific modules:
```bash
mvn install -pl compss/runtime/engine -am
mvn test -pl compss/runtime/scheduler/fullgraph
```

## Testing

**Setup (once per machine):**
```bash
# Configure passwordless SSH to localhost
cd ~/.ssh && ssh-keygen -t rsa   # accept defaults
cat id_rsa.pub >> authorized_keys

# Add loopback hostnames
sudo tests/scripts/configure_hosts_insert
```

**Running tests:**
```bash
cd tests/scripts

# All tests with a configuration
python3 tests.py -cfg ../configurations/NIO.cfg

# Single test by number
python3 tests.py -cfg ../configurations/NIO.cfg -t <test_number>

# Show all options
python3 tests.py -h
```

**Configuration file** (`tests/configurations/NIO.cfg`): Set `compss_home` to your installation directory before running tests.

**Docker-based testing** (used in CI):

The CI uses two separate images:
1. **`build` image** (root `Dockerfile`, `--target build`) — builds and installs COMPSs. Tests are **not** included here; `.dockerignore` intentionally excludes `tests/`.
2. **`tests` image** (`tests/Dockerfile`) — the test runner. Copies COMPSs from the `build` image via `COPY --from=build` and adds test sources, SSH, pre-compiled Java test JARs, and an entrypoint that runs `configure_hosts_insert` + starts sshd. Container runs as root.

To build and run locally:
```bash
# Step 1: build the COMPSs installation image, tag it as 'build'
docker build --target build -t build .

# Step 2: build the test runner image (context is the tests/ directory)
docker build -f tests/Dockerfile -t tests tests/

# Step 3: run tests via docker_test (all extra args pass through to tests.py)
./tests/scripts/docker_test -t 1        # single test by number
./tests/scripts/docker_test -f python   # entire family
```

**Useful `tests.py` flags** (append after `docker_test`):

| Flag | Meaning |
|------|---------|
| `-t <N>` | Run single test by global number; also accepts `family:N` or `family:name` |
| `-f <family>` | Run an entire family (`python`, `java`, `c`, `agents`, `agents_transfers`, `cloud`, `fault_tolerance`, `performance`, `socket`, `gos`, `pscos`, `tools`) |
| `-fj` / `-fp` / `-fc` | Shorthand for `-f java` / `-f python` / `-f c` |
| `-FF` | Fail-fast: stop on the first test failure |
| `-R` | No retry (run each test exactly once; default retries up to 3×) |
| `-r <1-5>` | Set explicit retry count |
| `--shard <K/N>` | Deploy/run only the K-th of N interleaved slices of the selected families (round-robin by family number); ignored when `-t` is given. Lets several containers run the same family in parallel |

**Container lifecycle:**
```bash
# If docker_test says the container already exists, remove it first:
docker rm -fv tests

# Pull logs out of a running/stopped container:
docker cp tests:/root/tests_execution_sandbox/logs ./local-logs

# Drop into a shell for manual debugging:
docker exec -it tests /bin/bash -l

# Tear down after you're done:
docker rm -fv tests
```

`docker_test` starts the container with `sleep infinity` (the entrypoint handles configure_hosts + sshd), then exec's `tests.py` passing through all arguments.

Sandbox output goes to `/root/tests_execution_sandbox/`. Java test apps are pre-compiled into the image at build time (`tests/Dockerfile` runs Maven over all `pom.xml` files), so the test runner skips recompilation and Maven downloads at runtime.

## Architecture

### Runtime (`compss/runtime/`)
The core framework. Key sub-components:
- `engine/` — Orchestrates task submission, the task dependency graph (DAG), and data management
- `scheduler/` — Pluggable task schedulers: `fullgraph/`, `lookahead/`, `orderstrict/`, `predefined/`
- `adaptors/` — Communication backends: `nio/` (main), `local/`, `gat/`, `http/`, `agent/`
- `commons/` — Shared types: tasks, data, resources, exceptions
- `checkpointer/` — Fault-tolerance checkpointing
- `tracing/` — Extrae tracing integration

### Language Bindings (`compss/programming_model/bindings/`)
Each binding wraps the runtime through `bindings-common/` (a C/C++ shared library):
- **Python (PyCOMPSs)** — `python/src/pycompss/`: `api/` (decorators like `@task`), `runtime/` (master-side), `worker/` (worker-side execution), `dds/` (distributed data structures), `ext/` (C extensions)
- **Java** — Direct JVM integration, no separate binding directory
- **C/C++** — Native execution via `bindings-common`
- **R (RCOMPSs)** — R package wrapping C bindings

### Agent Framework (`compss/agent/`)
Enables COMPSs to run as long-lived services that accept task submissions via REST or other protocols. Used for serverless and workflow orchestration scenarios.

### Tools (`compss/tools/`)
- `monitor/` — Web UI dashboard for runtime monitoring
- `tracing/` — Post-processing and visualization for Paraver traces

### Tests (`tests/sources/local/`)
Tests are organized by family (directory). Each test `N_name/` contains: `deploy`, `execution`, `result` scripts, and optionally a `pom.xml` for Java tests. The number prefix indicates test importance/order.

## Workflow Provenance

Workflow Provenance (`runcompss -p` / `--provenance`, `-z` / `--zip_provenance`) records the metadata of an application run as an RO-Crate (Workflow RO-Crate + Workflow Run RO-Crate profiles). The generator lives in `compss/runtime/scripts/system/provenance/`, but the data it needs is captured all over the runtime, and the log formats are a contract between all these parts:

- User scripts: `runcompss`, `launch_compss`, `enqueue_compss`; `system/runtime/compss_setup.sh`, `provenance.sh`, `profiler.sh`; `system/profiling/profiler.py`
- Java master: `COMPSsRuntimeImpl`, `WorkflowImpl`, `Task`, `TaskScheduler`/`JSONStateManager` (`dataprovenance.log`, logger `Loggers.DATA_PROVENANCE`)
- Java worker: `Executor`, `Invoker`, `NIOWorker`, `ResourceImpl`, `WorkerStarterCommand` (`worker_dp.out`)
- PyCOMPSs worker: `pycompss/runtime/task/worker.py` (`binding_dp.out`)
- Log configuration: `compss/runtime/config/log/COMPSsMaster-log4j*`, `COMPSsWorker-log4j*`, `pycompss/util/logger/cfg/logging_*.json`
- Tests: `tests/sources/local/tools/3_provenance_*`

**Before changing any of these, read `compss/runtime/scripts/system/provenance/AGENTS.md`**, which describes the end-to-end flow, the format of every log file and the rules to keep producers and parser in sync.

## Code Quality

**Java:** Checkstyle is enforced via Maven (`maven-plugins/checkstyle/`). Run with:
```bash
mvn checkstyle:check
```

**License headers:** All source files must have the Apache 2.0 license header. The Maven `validate` phase checks and adds headers automatically via `utils/scripts/header_setup/`.

**Python:** Bandit security checks are configured in `pyproject.toml` (in the PyCOMPSs binding directory). Style checks run as part of `buildlocal --python-style`.

## CI/CD (GitLab)

Pipeline defined in `tests/.gitlab-ci.yml`.

**GitLab interaction:** use the `glab` CLI for MRs, issues, pipelines, and other GitLab operations instead of raw REST calls. The API is served at `https://gitlab.bsc.es` (HTTPS); git remotes use `gitlab-internal.bsc.es` over SSH only.

**Merge request descriptions:** follow `PULL_REQUEST_TEMPLATE.md` (repository root). GitLab does not fill it in automatically, so copy its sections: `# Pull Request`, `## Status`, `## Description`, `## Type of change`, `## Project impact`, `## Related PRs`, `## How Has This Been Tested?`, `## Checklist`. Keep only the checkboxes that apply and tick what was done; put the test details (local checks, supercomputer job ids) under "How Has This Been Tested?".

**Documentation-only MRs:** when an MR changes only Markdown files (and the root `RELEASE_NOTES`, `changelog`, `ACKNOWLEDGMENTS`, `NOTICE`, `CITATION.cff`), `build` and `test` end at once with success (`.docs_only_skip` in `.gitlab-ci.yml`); `check` still runs. Any other changed file runs the full pipeline.

Stages:
1. `rebase_verify` — MR must be rebased on target branch; auto-rebases and pushes for same-repo MRs
2. `header_setup` — Validates Apache 2.0 license headers and Maven project structure; auto-fixes and pushes for same-repo MRs
3. `docker_build` — Three sub-jobs:
   - `docker_build_deps` (conditional) — rebuilds the slow `deps` image (Extrae/DLB/Kafka compilation) only when `Dockerfile`, `builders/deps-versions`, `builders/pre-install-deps`, `.gitmodules`, or dependency submodule pointers (`dependencies/dlb`, `dependencies/extrae`, `dependencies/kafka`) change
   - `docker_build_test_deps` (conditional) — rebuilds the `test-deps` image (apt/pip/Julia installs) only when `tests/Dockerfile` changes
   - `docker_build` — builds the `build` stage from root `Dockerfile` (tagged `ci-build:$CI_PIPELINE_ID`), tags it as `build`, then builds `tests/Dockerfile` (context = `tests/` dir) and pushes as `COMPSS_IMAGE` (`$CI_REGISTRY_IMAGE/ci:$CI_PIPELINE_ID`)
4. `tests` — Four parallel matrix jobs pulling `COMPSS_IMAGE` and running `docker_test`:
   - Job 1: `python` (~40 min sequential, ~20 min sharded — see below)
   - Job 2: `java` (~33 min)
   - Job 3: `cloud`, `fault_tolerance`, `pscos`, `agents_transfers` (~27 min)
   - Job 4: `c`, `tools`, `agents`, `performance`, `socket`, `gos` (~26 min)

   The python job additionally sets `TEST_SHARDS: "2"`: the job launches that many
   test containers in parallel on the same runner, each running
   `tests.py --shard K/N` (deploys/runs only the K-th of N interleaved slices of
   the selected families, round-robin by family number). Containers are fully
   isolated (own network namespace, `/root` and `/tmp` volumes), and app numbers
   are globally unique, so the per-shard log trees merge into one `logs/` artifact.
5. `cleanup_registry` — Deletes the per-pipeline `ci` image tag from the registry (runs even on failure)

**Key architectural details:**
- `tests/` is excluded from the root build context via `.dockerignore` — test sources live only in the `tests` image
- The `docker_build` job tags the `build` image as `build` locally before building `tests/Dockerfile` so `COPY --from=build` resolves without a registry push
- CI runs on shell executor runners (no `image:` directive); docker commands hit the host daemon directly
- Test artifacts (logs from `/root/tests_execution_sandbox/logs/`) are collected for 7 days even on failure

## Java Code Style

All Java sources are auto-formatted by `formatter-maven-plugin` (Eclipse formatter) and `impsort-maven-plugin` during `mvn validate`. Write code that already matches these rules to avoid a fixup commit.

**Key rules (from `eclipse-formatter-config.xml`):**

- **Line length:** 120 characters (`lineSplit=120`). Javadoc and comments wrap at 120 too (`comment.line_length=120`).
- **Indentation:** 4 spaces, no tabs (`tabulation.char=space`, `tabulation.size=4`).
- **Braces:** always end-of-line (`end_of_line` for all block types).
- **Blank lines:**
  - 1 blank line before each method (`blank_lines_before_method=1`)
  - 1 blank line before the first class body declaration (`blank_lines_before_first_class_body_declaration=1`)
  - At most **1** consecutive blank line preserved anywhere (`number_of_empty_lines_to_preserve=1`) — no double blank lines between members
  - 2 blank lines after imports (`blank_lines_after_imports=2`)
- **Imports:** single group, no sub-grouping (`groups=*`), unused imports removed, case-sensitive sort.
- **Comments:** ASCII only — the formatter rewrites non-ASCII characters in comments, so avoid Unicode arrows (`->` not `→`) and other non-ASCII symbols in inline comments and Javadoc.
- **Javadoc:** reformatted automatically; long `@param`/description lines wrap at 120 chars. Keep descriptions on one logical line — the formatter joins and re-wraps them.
- **`TaskScheduler.java` is excluded** from auto-formatting (listed in the formatter excludes).

**To verify before committing:**
```bash
mvn validate -pl <changed-module> --no-transfer-progress
```
A clean run reports `Successfully formatted: 0 file(s)` (nothing to reformat). If files are reformatted, stage and amend before pushing.

## Known Issues

### `9_stream_objects` flakiness (DistroStream race condition)
Between test iterations, COMPSs restarts its runtime including the Java DistroStream master server. The Python client (`streams/components/distro_stream_client.py`) can attempt to connect before the server is accepting connections, causing `[Errno 111] Connection refused`. The client handles this by retrying `ConnectionRefusedError` in `_process_request` (constants `_MAX_RETRIES` and `_RETRY_DELAY` in that file). If this error reappears, check that retry logic first.
