# AGENTS.md — Workflow Provenance

This file gives AI coding agents guidance on the **Workflow Provenance generation** feature of COMPSs. Read the root `AGENTS.md` first for general build, test and style rules. This file applies to everything under `compss/runtime/scripts/system/provenance/`, and also to the files elsewhere in the repository that capture provenance data (listed in [Where the code lives](#where-the-code-lives)).

## What it does

Workflow Provenance records metadata about the execution of a COMPSs application and packages it as an [RO-Crate](https://www.researchobject.org/ro-crate/). The crate follows the [Workflow RO-Crate](https://w3id.org/workflowhub/workflow-ro-crate/1.0) profile and the [Workflow Run RO-Crate (WRROC)](https://w3id.org/ro/wfrun) profiles, at one of two levels:

- **Provenance Run Crate** (WRROC level 3, the default): includes details of every task executed (method, parameters and their values, host, start and end times, status, job logs).
- **Workflow Run Crate** (WRROC level 2, the fallback): only overall information about the workflow run (inputs, outputs, status, agent, timings, resource usage), without task details. Used when the worker logs are missing or when `provenance_run: False` is set in the YAML.

The level is set in `wrroc/profile.py` (`set_profile_details`), which adds the matching `conformsTo` profiles.

Python (PyCOMPSs) and Java applications are supported, although Java applications still do not support generation of WRROC Provenance Run Crate level of detail.

**Design principle: capture is lightweight, processing is post-mortem.** During the run, the runtime only appends short lines to log files, so that recording provenance adds no noticeable overhead to the workflow. All the expensive work (parsing, file inspection, metadata building, plots) is done by the generator after the application ends. See [Automatic, Efficient and Scalable Provenance Registration for FAIR HPC Workflows](http://dx.doi.org/10.1109/WORKS56498.2022.00006) (WORKS 2022). Keep new capture code within that principle.

User documentation: https://compss-doc.readthedocs.io/en/latest/Sections/04_Ecosystem/05_Workflow_Provenance.html (sources in the `wdc/documents/documentation` repository, under `COMPSs_Manuals/source/Sections/04_Ecosystem/05_Workflow_Provenance/`).

## How it is enabled

Flags of `runcompss` and `enqueue_compss`:

| Flag | Effect |
|------|--------|
| `-p`, `--provenance` | Generate the crate in a regular folder. Uses `ro-crate-info.yaml` from the working directory. |
| `--provenance=<yaml>` | Same, with a specific YAML file describing the workflow (authors, license, source files, inputs/outputs, ...). |
| `-z`, `--zip_provenance` | Generate a single `.zip` file with the crate content instead of a folder. |
| `--provenance_folder=<path>` | Path where the crate is generated. Default: `COMPSs_RO_Crate_<YYYYMMDD_HHMMSS>/` in the working directory. |

Any of these flags enables provenance (`provenance=true` in the scripts) and also enables `--graph`, because the workflow diagram (`complete_graph.svg`) is added to the crate. The scripts pass `-Dcompss.data_provenance=true` to the JVM (`compss_setup.sh`), and the same value reaches the workers through `WorkerStarterCommand` and the PyCOMPSs binding (`GLOBALS.get_data_provenance()`).

If the YAML file does not exist, the generator writes a template to `ro-crate-info_TEMPLATE.yaml` (content in `utils/yaml_template.py`) and records the run with a generated name. If author or organisation details are completed from online searches (ORCID, ROR), an updated copy is written to `GENERATED_<yaml>`.

## End-to-end flow

1. **Launch**: `runcompss` parses the flags, starts resource profiling (`start_profiling` in `runtime/profiler.sh`) and launches the application with data provenance enabled. With queue systems, `enqueue_compss` exports its arguments in `ENQUEUE_COMPSS_ARGS` and the job runs `launch_compss`, which in turn runs `runcompss` on the master node, so the rest of the flow is the same.
2. **Capture during the run**: the master runtime, every worker JVM and the PyCOMPSs binding write provenance records to dedicated log files (see next section), and a profiler process on every host samples resource usage.
3. **Application end**: worker logs are "frozen" (renamed to `static_<file>` by `NIOWorker`) and copied back to the master log folder, which happens whenever debug or provenance is enabled (`ResourceImpl.generateAndRetrieveWorkerDebug`).
4. **Generation**: `runcompss` registers `clean_env` with `trap clean_env EXIT`, so it runs however the application ends. `clean_env` calls `run_provenance` (`runtime/provenance.sh`), which:
   - writes the submission command to `.compss_submission_command_line.txt`;
   - stops profiling and moves the `*.csv` profiling files from `workers/` to `stats/` (the master's CSV gets a `-MASTER` suffix);
   - renders `monitor/complete_graph.dot` to SVG with `compss_gengraph` (skipped if the graph has 6500 edges or more);
   - runs `python3 -O generate_COMPSs_RO-Crate.py <yaml> <log_dir> <dest_folder> <zip_bool>` with `PYTHONPATH=${COMPSS_HOME}/Runtime/scripts/system/`.
5. **Cleanup**: if generation succeeds, the temporary command line file and the SVG are removed. If it fails, they are kept and the script prints the commands to re-run the generation by hand.

Generation also runs when the application fails, so that failed runs are recorded. The workflow status (`CompletedActionStatus`, `FailedActionStatus`, `PotentialActionStatus`, `ActiveActionStatus`) is computed from the task statuses and from `COMPSS_EXIT_CODE`, exported by `compss_setup.sh`. Exit codes 130, 137, 143 (SIGINT, SIGKILL, SIGTERM) and 122 (wall clock limit reached) map to `PotentialActionStatus`.

## Log files the generator relies on

All paths are relative to the application log folder (`<log_dir>`), by default `~/.COMPSs/<app_name>_<NN>/` (changed with `--log_dir`).

### `<log_dir>/dataprovenance.log` (master)

Written by the Java logger `es.bsc.compss.DataProvenance` (`Loggers.DATA_PROVENANCE`), configured in `compss/runtime/config/log/COMPSsMaster-log4j*` (`COMPSsDataProvenanceLog` appender, message-only pattern). It contains, in order of appearance:

| Content | Written by | Format |
|---------|------------|--------|
| COMPSs version, application name, start time | `COMPSsRuntimeImpl` (start) | one value per line |
| Files and directories accessed by the application | `WorkflowImpl` | `file <name> <type> <url> <access_mode>` |
| Tasks submitted and their parameters | `WorkflowImpl` | `task <id> <signature> <name>.<type>.<access_mode>::...` |
| Task status updates | `Task` | `task <id> status <STATUS>` |
| End time | `COMPSsRuntimeImpl` (stop) | ISO timestamp |
| Execution statistics per resource and implementation | `TaskScheduler` → `JSONStateManager.writeDataProvenance` | `<resource> <implementation> <stat> <value>` (fixed-width columns) |
| Fatal error | `COMPSsRuntimeImpl.handleFatalError` | `master status FAILED` |

It is parsed by `processing/entities.py` (`get_main_entities`: version, name, times) and `processing/master_log.py` (`process_master_log`: inputs, outputs and tasks), and the statistics by `wrroc/create_action.py` (`get_stats_list`). If this file is missing or empty, generation aborts.

Task statuses are `FINISHED`, `RECOVERED` (restored from a checkpoint), `FAILED`, `CANCELED` and `UNKNOWN`. A resubmitted task logs several statuses, and `STATUS_PRIORITY` in `master_log.py` decides which one is kept.

### `<log_dir>/workers/<hostname>/Log/` (one folder per worker)

Required only for the Provenance Run Crate level:

- **`static_worker_dp.out`**: Java side of the worker (`COMPSsWorker-log4j*`, file `worker_dp.out`). `Executor` writes `TASK=<id> STARTTIME=<iso>`, `TASK=<id> ENDTIME=<iso>` and `TASK=<id> LOGFILE=<job_log>.out|.err` for every task. For Java tasks, `Invoker.logProvenanceOfParameters` also writes one `TASK= JOB= HOST= METHOD= PARAMETER= BASICTYPE= COMPSSTYPE= CONTENT= DIRECTION=` line per parameter. Even in Python applications, `@binary` and `@software` tasks are executed by the Java worker only (the Python worker is never invoked), so their parameters are recorded only in this file.
- **`static_binding_dp.out`**: PyCOMPSs binding (`dp_logger` in `pycompss/util/logger/cfg/logging_worker.json`, file `binding_dp.out`). For Python tasks, `PythonInvoker` overrides `logProvenanceOfParameters` with an empty method, and `pycompss/runtime/task/worker.py` writes one line per parameter and return value instead, with `TASK= HOST= METHOD= PARAMETER= COMPSSTYPE= BASICTYPE= IS_ARRAY= DESCRIPTION= CONTENT= DIRECTION=`. This gives the actual Python types and values, and the host where each method ran. This file does not exist for Java applications.

Both are `KEY=VALUE` lines parsed by `processing/worker_logs.py` (`update_tasks_from_worker_logs`), which completes the `Task` objects built from the master log.

If both files are missing from every worker, the generator falls back to the Workflow Run Crate level and prints a warning. If only one file is missing for a host, it warns that task information may be limited. Logs must survive the end of the run, whether the application ends correctly or with an error, for task details to be recorded.

### Other inputs

- `<log_dir>/monitor/complete_graph.svg`: workflow diagram (added as the workflow image).
- `<log_dir>/jobs/`: job logs, linked to their task when known. They exist when debug is enabled or when tasks fail.
- `<log_dir>/stats/`: resource profiling CSVs and summaries, and the generated plots (see next section).
- `<log_dir>/energy/`: energy measurements (EAR CSVs), when available.
- `<log_dir>/trace/`: Paraver traces, when tracing is enabled.
- `.compss_submission_command_line.txt` in the working directory.

## Resource profiling

`runtime/profiler.sh` launches `profiling/profiler.py` on every host involved in the computation:

- On the master (only if it does not also run a worker, `worker_in_master_cpus` empty or 0), from `runcompss`.
- On every worker, from `compss/runtime/adaptors/nio/scripts/persistent_worker_starter.sh`.
- Locally as a background process, or with `srun --overlap` when running under SLURM (`ENQUEUE_COMPSS_ARGS` set).

The profiler samples CPU and memory usage, network bytes sent and received (including InfiniBand counters when present), disk I/O and, if a supported GPU is found (NVIDIA, AMD, Intel or Apple), GPU and GPU memory usage. CSV columns: `CPU,MEM,BYTE_SENT,BYTE_RECV,BYTE_READ_DISK,BYTE_WRITE_DISK,TIME_READ_DISK,TIME_WRITE_DISK,TIME`, plus `GPU_USAGE,GPU_MEM` when a GPU is present. It writes `resource_profiling_<hostname>.csv` and, when it stops, `profiling_summary_<hostname>.log`. Worker files are frozen like the other worker logs, so in `<log_dir>/stats/` they appear as `static_resource_profiling_<hostname>.csv` (and the master's as `resource_profiling_<hostname>-MASTER.csv`). On macOS the profiler requires `psutil`, since `/proc` is not available.

The polling interval is 5 seconds by default and can be changed with the environment variable `COMPSS_PROFILING_INTERVAL` (in seconds). The profiler stops when it receives `SIGUSR1` (`stop_profiling`), with a forced kill after a timeout in local runs.

With these metrics the generator adds resource usage information to the crate and calls `wrroc/profiling_plots.py`, which runs the `compss_genprofiling` tool (`compss/tools/resource_analysis_plots/`) in silent mode with `--scope=aggregated`. Only the aggregated plots (`cpu_aggregated.svg`, `mem_aggregated.svg`, and `gpu_usage_aggregated.svg`, `gpu_mem_aggregated.svg` when GPU data exists) are generated and added to the `profiling/` folder of the crate. On single-node runs there is nothing to aggregate, so the tool writes the plots of that node instead (`cpu.svg`, `mem.svg`, `gpu_usage.svg`, `gpu_mem.svg`), and their descriptions name the node from its `resource_profiling_<node>.csv` file. Outlier hosts are highlighted in the plot legends (IQR method for 5 workers or more, ±30% of the median otherwise), so the user can spot hosts that behave differently from the rest. Per-node plots are not generated by default: users can run `compss_genprofiling` by hand on the `stats/` folder for those.

## The YAML file (`ro-crate-info.yaml`)

Main keys under `COMPSs Workflow Information`, read by `generate_COMPSs_RO-Crate.py`, `processing/entities.py` and `file_adding/`:

| Key | Meaning | Default |
|-----|---------|---------|
| `name`, `description`, `license` | Workflow metadata | generated name |
| `sources`, `sources_main_file` | Application source files and directories to include, and the main file | detected main file |
| `data_persistence` | Copy input and output files into the crate (otherwise only referenced, with `localPath`) | `False` |
| `trace_persistence` | Copy Paraver traces into the crate | `False` |
| `runtime_logs_persistence` | Include COMPSs runtime logs | `True` |
| `inputs`, `outputs` | Inputs and outputs added by hand, besides the detected ones | — |
| `software` | Software dependencies (`name`, `version`, `url`) | — |
| `provenance_run` | Enable the Provenance Run Crate level | `True` |
| `param_size_limit` | Maximum number of characters stored for a parameter value | `1500` |

Notes on how these keys are used:

- Without `sources`, only the application's main file is added (if found in the working directory). Every file under the directories listed in `sources` is added, not only source code.
- `inputs` and `outputs` also accept remote `https://` URLs, to reference large datasets published elsewhere (e.g. Zenodo) without copying them.
- With `data_persistence: True`, files are copied under `dataset/`, keeping the folder structure below the longest common path of all inputs and outputs (`utils/common_paths.py`). Files without a common root are placed separately, and directories passed as COMPSs `DIRECTORY` parameters are always treated as separate locations.

The top-level keys `Authors` and `Agent` describe the people who wrote and ran the workflow (`name`, `e-mail`, `orcid`, `organisation_name`, `ror`). If `Agent` is missing, the first author is used. When only a `name` or an `orcid` (or only an organisation name or `ror`) is given, `processing/entities.py` completes the rest through the ORCID and ROR public APIs, and values given by the user override what is found online. These lookups need outbound network access, which many clusters block, so they must stay optional: a failed or slow lookup must never stop the generation.

The full template, with comments, is in `utils/yaml_template.py`. Keep it in sync with the user documentation when adding keys.

## The generated crate

A folder `COMPSs_RO_Crate_<timestamp>/` (or `.zip`) in the working directory, with:

| Path in the crate | Content |
|-------------------|---------|
| `ro-crate-metadata.json` | The RO-Crate metadata (JSON-LD). |
| `application_sources/` | Files from `sources`: directories keep their folder structure, single files go to the root of this folder. |
| `complete_graph.svg` | Workflow diagram (if generated). |
| `<yaml>` | The YAML file used. |
| `compss-<job_id>.out`, `compss-<job_id>.err` | Standard output and error of the job, only on SLURM clusters. |
| `dataset/` | Inputs and outputs, only with `data_persistence: True` (otherwise they are referenced with `localPath`). |
| `profiling/` | Aggregated resource usage plots. |
| `trace/` | Paraver traces, only with `trace_persistence: True`. |
| `runtime_logs/` | COMPSs runtime logs, keeping the structure of `<log_dir>` (unless `runtime_logs_persistence: False`), including the profiling CSVs under `runtime_logs/stats/`. Job logs of tasks go under `runtime_logs/jobs/`. |

The workflow diagram and the YAML file get a `sha256` checksum in the metadata (`file_adding/source_code.py`).

### Consumers of the crate

Other tools read the generated crates and depend on their layout and on the metadata terms used. Changing either may break them:

- `pycompss inspect` in the PyCOMPSs CLI (developed in a separate repository: https://gitlab.bsc.es/wdc/compss/tools/cli).
- The Reproducibility Service ([`compss/tools/reproducibility_service/`](../../../../tools/reproducibility_service/)), which re-runs an experiment from a crate.
- Publication in WorkflowHub, Zenodo and ROHub, described in the user documentation.

## Where the code lives

### RO-Crate generator (this folder, installed as `${COMPSS_HOME}/Runtime/scripts/system/provenance/`)

| Path | Purpose |
|------|---------|
| `generate_COMPSs_RO-Crate.py` | Entry point (`main`). Reads the YAML and logs, decides the WRROC level, builds and writes the crate. |
| `processing/master_log.py` | Parses `dataprovenance.log`: workflow inputs/outputs and `Task`/`Parameter` objects. |
| `processing/worker_logs.py` | Parses `static_*_dp.out` and completes the tasks. |
| `processing/entities.py` | Root entity, main entity, authors and organisations (ORCID/ROR lookups), software requirements. |
| `models/` | `Task` and `Parameter` dataclasses. |
| `file_adding/source_code.py` | Adds the application source files. |
| `file_adding/datasets.py` | Adds input/output files and directories, logs, traces, statistics and plots. |
| `wrroc/create_action.py` | Main `CreateAction` of the run: status, agent, timings, resource usage. |
| `wrroc/provenance_run/` | Provenance Run Crate entities: `prospective.py` (`HowToStep`, formal parameters, tools) and `retrospective.py` (`CreateAction`, `ControlAction`, `OrganizeAction` per task). |
| `wrroc/profile.py` | `conformsTo` profiles for the chosen level. |
| `wrroc/profiling_plots.py` | Runs `compss_genprofiling` to generate the plots. |
| `wrroc/store_data.py` | Writes the execution and profiling statistics of the crate to `<log_dir>/stats/stats.json`. |
| `utils/` | Common path detection, COMPSs to RO-Crate type mapping, URL fixes, YAML template. |

### Capture side (outside this folder)

| Path | Role |
|------|------|
| `compss/runtime/scripts/user/runcompss`, `launch_compss`, `enqueue_compss` | Provenance flags; start profiling; call `run_provenance` at the end. |
| `compss/runtime/scripts/system/runtime/compss_setup.sh` | `-Dcompss.data_provenance` JVM option; `COMPSS_EXIT_CODE`. |
| `compss/runtime/scripts/system/runtime/provenance.sh` | `run_provenance`: post-run steps and generator call. |
| `compss/runtime/scripts/system/runtime/profiler.sh`, `../profiling/profiler.py` | Resource profiling. |
| `compss/runtime/adaptors/nio/scripts/persistent_worker_starter.sh` | Starts and stops profiling on workers. |
| `compss/runtime/commons/.../COMPSsConstants.java`, `COMPSsDefaults.java`, `log/Loggers.java` | `compss.data_provenance` property, default, `DATA_PROVENANCE` logger name. |
| `compss/runtime/config/log/COMPSsMaster-log4j*`, `COMPSsWorker-log4j*` | Appenders for `dataprovenance.log` and `worker_dp.out`. |
| `compss/runtime/engine/.../api/impl/COMPSsRuntimeImpl.java`, `WorkflowImpl.java`, `types/Task.java`, `types/AbstractTask.java` | Master records (version, times, files, tasks, statuses, failure). |
| `compss/runtime/scheduler/commons/.../TaskScheduler.java`, `util/JSONStateManager.java` | Execution statistics at shutdown. |
| `compss/runtime/execution/core/.../executor/Executor.java`, `invokers/Invoker.java` | Worker records (times, job logs, parameters). |
| `compss/runtime/adaptors/nio/worker/.../NIOWorker.java`, `compss/runtime/resources/commons/.../ResourceImpl.java`, `adaptors/engine/.../WorkerStarterCommand.java` | Propagate the flag to workers, freeze and retrieve worker logs. |
| `compss/programming_model/bindings/python/src/pycompss/runtime/task/worker.py`, `util/logger/cfg/logging_*.json` | PyCOMPSs binding records (`binding_dp.out`). |
| `compss/tools/resource_analysis_plots/` | `compss_genprofiling` plotting tool. |
| `compss/tools/reproducibility_service/` | Consumer of the generated crates: reproduces a run from a crate. |

## Rules for changes

- **Producers and parsers must stay in sync.** The log lines are a contract between Java, Python binding and generator code. When changing a field, a separator or a file name on one side, update the parser (`processing/master_log.py`, `processing/worker_logs.py`, `wrroc/create_action.py`) in the same change, and update this file.
- **Keep the copies in sync.** User-visible changes (flags, YAML keys, crate layout, messages, dependencies) must also be reflected in the user documentation (see the link at the top of this file).
- **Never let provenance break the application.** Capture code must be guarded by the data provenance flag (`DP_ENABLED`) and must not raise. The generator runs after the application, so its failures must not change the application exit code.
- **Prefer degrading to failing.** Missing worker logs, files that no longer exist or unparsable lines produce a `PROVENANCE | WARNING:` message and less detailed metadata, not an exception. Keep that behaviour.
- **Mind the size of the crate and the generation time.** Large workflows can have many thousands of tasks and files. Avoid per-task work that is quadratic or that touches the file system repeatedly. Parameter values are truncated to `param_size_limit`.
- **Output style.** Messages start with `PROVENANCE | ` (`PROVENANCE | PROFILING | ` for profiling). Debug-only messages go inside `if __debug__:`. The generator runs with `python3 -O` unless `COMPSS_PROV_DEBUG` is set, so those messages are hidden by default.
- **Dependencies.** The generator uses `rocrate` (ro-crate-py 0.13.0 or later; the code handles versions before and after 0.15), `PyYAML`, `pytz`, `requests` (ORCID/ROR lookups), `pandas` (resource usage, energy data and profiling plots) and `matplotlib` (through the plotting tool). The profiler uses only the standard library on Linux and `psutil` on macOS. CI pins the ro-crate-py version with `ROCRATE_VERSION` in `tests/Dockerfile`. Do not add new dependencies without updating the user documentation (Software dependencies section).
- **RO-Crate validity.** Generated crates must stay valid for the declared profiles. Check them with `rocrate-validator` (see Checking a generated crate).

## Testing

- Integration tests in the `tools` family: `tests/sources/local/tools/3_provenance_basic` (checks the generated metadata with `src/check_ro_crate_metadata.py`) and `tests/sources/local/tools/3_provenance_rocrate-validator` (validates the crate with `rocrate-validator`). Run them with `python3 tests.py -cfg ../configurations/NIO.cfg -t tools:3_provenance_basic` (see the root `AGENTS.md`).
- Manual run: execute any PyCOMPSs application with `runcompss -p` (add `-d` to keep the job logs), check the warnings printed with the `PROVENANCE |` prefix, and inspect `ro-crate-metadata.json` in the generated folder.
- Re-running the generator without re-running the application (the commands are printed by `run_provenance`):

```bash
export PYTHONPATH=${COMPSS_HOME}/Runtime/scripts/system/:$PYTHONPATH
python3 ${COMPSS_HOME}/Runtime/scripts/system/provenance/generate_COMPSs_RO-Crate.py \
    ro-crate-info.yaml ~/.COMPSs/<app_name>_<NN>/ COMPSs_RO_Crate_test/ false
```

  `COMPSS_EXIT_CODE` can be exported beforehand to reproduce the status of a failed run. Run without `-O` to see the `PROVENANCE DEBUG` messages.

### Checking a generated crate

Use all three methods below: they check different things. The validator only checks the schema and `pycompss inspect` shows different summaries, so neither can tell whether the whole content is *true*. Only the third one does. These checks only read the crate, so run them from outside the application folder.

1. **Schema validity** with `rocrate-validator`:

   ```bash
   rocrate-validator validate --no-paging -f text <crate>                 # REQUIRED level: must pass
   rocrate-validator validate --no-paging -f json -l recommended <crate>  # group the failures
   ```

   Most RECOMMENDED failures are known and cosmetic: singleton arrays, `scp://` URLs that cannot be resolved, missing `encodingFormat`, and per-task actions without `agent` or `description`.

2. **Summary** with `pycompss inspect <crate> [flags]`:

   ```bash
   pycompss inspect <crate> -v
   pycompss inspect <crate> -v -d
   pycompss inspect <crate> -f
   pycompss inspect <crate> -t 1,2,8      # optionally -m <method>[,<method>...]
   ```

   The most essential information to check when not using any flags is the `Status` and `Scheduled Tasks`. The workflow's overall inputs and otputs can be checked in detail with the -d flag. Each task's details (i.e. -t flag) also shows as key information the `Inputs` and `Outputs` of the task, but also its `Status`, the `Method` it ran and the `Host` where the task ran. `Start Time` and `End Time` can be both used to calculate `Execution Time`, but also to compare it with other tasks, to understand which tasks ran in parallel at the same time, and what was their execution order. These times account only for user code execution time, and no COMPSs runtime overheads are included (i.e. creating and submitting the task, or its finalisation).

3. **Content** with a script that parses `ro-crate-metadata.json`:
   - diff the local `File`/`Dataset` entities against the directory contents, including sizes and modification dates (when available);
   - check the overall (i.e. in the main `CreateAction`) and each task's `object`/`result` lists against the application's algorithm;
   - check the YAML fields, the command line and the timestamps.

Expected behaviour, not bugs:
- The `./` entry in the `result` of the main `CreateAction` is intentional (`wrroc/create_action.py`, "The generated RO-Crate").
- INOUT files show their final size even when listed as inputs (a known pending item).
- The runtime profiling statistics (average/min/max per method) include runtime overheads. Each task's `startTime`/`endTime` covers only the code execution, so differences on those times can be normally appreciated (i.e. the difference accounts for runtime overhead). The smaller the task, the larger the overheads appreciated can be.
