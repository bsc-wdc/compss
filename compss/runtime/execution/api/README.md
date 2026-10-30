# compss-execution-api

Technical documentation of the `api` submodule of
[compss-execution](../README.md).

## Purpose

`compss-execution-api` defines the shared contracts and types used by every
participant of the task execution process: the runtime components that build
task descriptions, the engine that executes them, the language invoker plugins
that run the actual user code, and the test infrastructure that verifies both.

This module contains interfaces, enums and exception types only. It has no
implementation: the execution engine (`core`), the language plugins
(`plugins`) and the test utilities (`test-commons`) implement or mock these
contracts.

All the packages of this module live under the `es.bsc.compss.execution`
root, which it shares with the engine module `core`: the api-owned contracts
described here and core's engine classes are split across their respective
jars under the same package root.

## Responsibilities

- Describe a task execution request (`Invocation`) decoupled from any specific
  programming language or invocation mechanism.
- Describe the parameters of an invocation and their data-management
  properties (`InvocationParam` and its collection and URI variants).
- Describe the execution environment where an invocation must run
  (`InvocationContext`): worker configuration, data management, nested task
  support and streaming setup.
- Define the thread binding contract (`ThreadBinder`) to assign computing
  units to jobs, together with its configuration constants.
- Define the exceptions raised during job execution and data management.
- Provide the tracing event types emitted by the execution engine for task and
  function execution, and the `JobHistory` state enumeration.

## Functionalities

### Package `es.bsc.compss.execution`

Core contracts of an invocation:

- `Invocation`: a task execution request. Provides the job and task
  identifiers, the task type, the method implementation to run, the task
  parameters, target and results, the resource requirements, the failure
  policy, the timeout and the parallelism detection source. It also lets the
  engine register execution start/end.
- `InvocationParam`: a task parameter. Describes its data type, value, value
  class, data-management identifiers, source URIs, renaming information,
  weight, STD IO stream usage and whether its result is computed by a nested
  task (`resultIsForwarded` / `isForwardedResult`).
- `InvocationParamCollection`: an `InvocationParam` that groups a list of
  internal parameters (element of a collection parameter).
- `InvocationParamURI`: a physical location of a parameter data: protocol,
  path and host check.
- `InvocationContext`: the environment of an invocation. Groups the worker
  configuration (host name, install/working/log/analysis/application
  directories), the execution configuration (task execution type, persistent
  C worker, language-specific parameters), the execution management
  (output registration, standard streams, thread log streams), data
  management (storage configuration, streaming backend and master location,
  parameter load/store), nested execution handling (`COMPSsRuntime` access,
  idle/reactivated reserved resource notifications), the environment script
  path and the EAR and data provenance flags.
- `LanguageParams`: marker interface for language-specific parameter bundles,
  retrieved from the context via `getLanguageParams(Lang)`.
- `ThreadBinder`: contract to bind and release computing units for a job,
  with the binder policy constants `disabled`, `automatic` and `dlb`.

### Package `es.bsc.compss.execution.exceptions`

Exception hierarchy for job execution and data management:

- `JobExecutionException`: error during the execution of a job.
- `InitializationException`: error during component initialization.
- `InvalidMapException`: error on a map structure handling.
- `UnsufficientAvailableResourcesException` and its subclass
  `UnsufficientAvailableComputingUnitsException`: a resource request cannot
  be satisfied.
- `NonExistentDataException` and its subclass
  `NonExistentElementException`: referenced data does not exist;
  both expose the list of missing data names via `getMissingData()`.
- `UnloadableValueException`: a parameter value cannot be loaded into memory.
- `UnwritableValueException` and its subclass `UnwritableValuesException`: a
  parameter value cannot be persisted; the latter aggregates the individual
  per-value failures.

### Package `es.bsc.compss.execution.job`

- `JobHistory`: state of a job in its lifecycle: `NEW`, `RESUBMITTED_FILES`,
  `RESUBMITTED`, `FAILED` and `CANCELLED`.

### Package `es.bsc.compss.execution.tracing`

- `TaskExecutionEvent`: tracing events emitted while a task runs on the
  worker (task running, task sandbox creation/removal, original-name
  binding/unbinding, OUT parameter check, nested class instrumentation and
  task timeout).
- `FunctionExecutionEvent`: base type for extensible tracing events
  describing the execution of a function, as emitted by the invocation
  language instrumentation.

## Dependencies

This module has no dependency on its sibling modules inside
compss-execution (`core`, `plugins`, `test-commons`); it is the contract they
all build upon. It only depends on framework types defined outside the module:

- [compss-commons](https://gitlab.bsc.es/wdc/compss/framework/-/tree/trunk/compss/runtime/commons):
  semantic and resource types referenced by the contracts (`FailurePolicy`,
  `AbstractMethodImplementation`, `TaskType`, `ResourceDescription`,
  `DataType`, `StdIOStream`, `COMPSsConstants`).
- [compss-runtime-api](https://gitlab.bsc.es/wdc/compss/framework/-/tree/trunk/compss/runtime/api):
  `COMPSsRuntime`, used by `InvocationContext` to expose nested task
  execution.
- [distro-stream-lib](https://gitlab.bsc.es/wdc/compss/framework/-/blob/trunk/compss/runtime/execution/api/pom.xml):
  `StreamBackend`, used by `InvocationContext` to describe the streaming
  backend.
- Tracing API (`es.bsc.wdc.tracing`): `Event`, `EventType` and `Tracer`,
  consumed by the tracing event types.
