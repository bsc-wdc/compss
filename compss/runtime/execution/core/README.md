# compss-execution-core

Technical documentation of the `core` submodule of
[compss-execution](../README.md). For a runtime-level description of
how the components interact, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Purpose

`compss-execution-core` is the execution engine of compss-execution: it hosts
the execution of task invocations on a node. The engine receives invocation
requests, reserves the local resources they need, resolves the language
invoker that runs the user code (through the invoker plugin SPI), manages the
executors that run requests, and exposes part of the node state as commands
for out-of-process executors.

## Entry point

The entry point of the engine is `ExecutionManager`
(`es.bsc.compss.utils.execution`): the facade that a worker uses to manage
task execution on its node. It builds the node `ResourceManager` and the
configured `ThreadBinder`, creates the `ExecutionPlatform` thread pools that
host the executions (one for tasks, one for IO-bound operations), and exposes
the node lifecycle to the runtime: `init()`, `enqueue()` of invocations,
capability adjustment, job cancellation and `stop()`. Every other component
of the engine is reached from it. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the resulting execution
flow.

## Responsibilities

- Manage a pool of `Executor` threads that process execution requests
  (`ExecutorRequest`) submitted by the runtime.
- Run task invocations through language invokers obtained from the
  `InvokerFactory` SPI, implemented by the `plugins` submodule.
- Reserve and release the computing resources (CPUs, GPUs, FPGAs, others)
  assigned to each job, with optional thread-to-core binding.
- Mirror the platform state to out-of-process executors through a pipe or a
  persistent process, using a command protocol (`ExternalCommand`).
- Support nested task execution: an invocation whose code detects more
  parallelism can register a workflow with the runtime API and become a
  nested application.

## Functionalities

### Package `es.bsc.compss.executor` — engine threads

- `Executor`: a platform thread that processes execution requests. Its main
  loop obtains requests from the platform and runs them; it executes
  invocations by creating the task sandbox, acquiring resources, obtaining an
  invoker from `InvokerFactory.Registry` and running it, and closing the
  sandbox afterwards. It also emits tracing events and records data
  provenance parameter logs when enabled.
- `InvocationRunner`: callbacks used by an invoker to notify that the code it
  hosts is stalled (resources can be released for other tasks) and that it is
  ready to continue (resources must be re-acquired).
- `CpuAffinity`: binds the calling thread to a set of CPUs with a correct
  mask for nodes with more than 64 CPUs.

### Package `es.bsc.compss.executor.external` — external executor protocol

- `ExternalExecutor<T>`: contract to send and read `ExternalCommand`s to and
  from an out-of-process executor.
- `ExecutorContextMirror<E>`: a mirror of the platform state (files, objects,
  task groups, barriers) kept on the external side.
- `commands`: one class per protocol command (task execution and cancellation,
  task group management, file and object operations, barrier creation,
  channel management, worker and executor lifecycle, alive/PID
  queries and error notification).
- `piped`: `PipedExecutor`, the pair of pipes (`PipePair`, `ControlPipePair`)
  driving an external executor process; `PipedMirror` plus `MirrorMonitor`
  track the state of the mirrored elements, and `piped.commands` provides the
  pipe-level encoding of the protocol commands.
- `persistent`: `PersistentExecutor` and `PersistentMirror`, variant where
  the external executor is a long-lived process that keeps its state between
  invocations.

### Package `es.bsc.compss.invokers` — invoker framework

- `Invoker<W>`: base class for language invokers. Parses the invocation
  parameters and computing-unit requirements, sets up the execution
  environment variables, runs prolog/epilog binaries, emits task tracing
  events, logs data provenance of parameters, and manages nested
  applications (workflow registration, input/output value handling). Also a
  `WorkflowListener` to release and re-acquire resources on nested task
  synchronizations.
- `InvokerFactory`: SPI entry point. Its `Registry` discovers factories
  through `java.util.ServiceLoader`, offers `preLoad` to prepare the
  infrastructure for a method type and language, and `getInvoker` to build
  the invoker for a specific invocation.
- `external`: `ExternalInvoker`, base invoker that delegates execution to an
  out-of-process executor through the `ExternalExecutor` protocol, with the
  `piped` and `persistent` variants.
- `types`: language-specific parameter bundles `CParams`, `JavaParams`,
  `PythonParams` and `RParams`, plus the `StdIOStream` type.
- `util`: `BinaryRunner` builds and runs binary commands (with optional
  failure-by-exit-value semantics); `CExecutionCommandGenerator` and
  `ClassUtils` support C and Java method invocation.

### Package `es.bsc.compss.execution` — platform

- `ExecutionPlatform`: the computing platform of a node. Hosts the thread
  pool of executors, receives `ExecutorRequest`s (invocation execution,
  activation and stop), assigns requests to threads, manages the thread
  count, tracks running jobs with their timeouts, cancels jobs, and starts
  mirrors for external executors. Implements the `ExecutorContext` type seen
  by invokers.
- `ExecutionPlatformConfiguration`: static description of the platform
  (thread count, CPU/GPU/FPGA/other computing units, binding maps).
- `types`: `ExecutorContext` (the platform-side view of an invocation
  context), `InvocationResources` (the concrete CPUs/GPUs assigned to an
  invocation) and request descriptions (`ExecutorRequest`,
  `InvocationExecutionRequest`, `ActivateExecutorRequest`,
  `StopExecutorRequest`, `InvocationExecutionListener`).

### Packages `es.bsc.compss.execution.utils` and `es.bsc.compss.utils.execution`

- `ResourceManager`: allocates and releases computing units per job from the
  node description, including re-acquisition with a preferred allocation and
  pending requests when resources are exhausted.
- `JobQueue`: utility to enqueue job execution requests.
- `ExecutionManager`: facade used by a worker to manage the execution of
  invocations on a node: initializes the platform with a
  `ResourceManager` and the configured `ThreadBinder`, enqueues invocations
  for execution, adjusts capabilities, cancels jobs and stops the platform.
- `ThreadedPrintStream`: redirects one `PrintStream` to different files
  according to the thread.
- `ThreadedProperties`: allows different properties per thread.

### Package `es.bsc.compss.binders` — thread binding

`ThreadBinder` implementations used by the resource manager:

- `BindToMap`: binds threads to a specific resource map (obtained from
  `lscpu` or given by the user).
- `BindToResource`: binds threads to the resource hosting as many cores as
  the given computing units.
- `Unbinded`: no thread affinity.

## Dependencies

- [compss-execution-api](../api/README.md): the contracts implemented by
  the engine (invocations, parameters, context, exceptions, tracing events).
- [compss-commons](https://gitlab.bsc.es/wdc/compss/framework/-/tree/trunk/compss/runtime/commons):
  semantic and resource types, method implementation definitions and the
  logging configuration.
- [compss-runtime-api](https://gitlab.bsc.es/wdc/compss/framework/-/tree/trunk/compss/runtime/api):
  `COMPSsRuntime`, for nested task execution (`registerWorkflow`).
- Tracing API: the `wdc-tracing-api` artifact (`es.bsc.wdc.tracing`), pulled
  transitively through compss-commons, used to emit task and affinity events.
- Third-party: log4j2, JNA, JSON (`org.json`) and distro-stream
  (server/client) for the streaming features exposed by the context.
