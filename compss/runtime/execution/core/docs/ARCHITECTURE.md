# compss-execution-core architecture

High-level architecture of the `core` submodule of
[compss-execution](../../README.md). This document describes how the
engine components interact at runtime and how their interfaces and
implementations relate; the per-package functionality inventory lives
in the [core README](../README.md).

## Overview

The engine runs on every worker node of a COMPSs deployment. Its entry point
is the `ExecutionManager` facade, which owns the thread pools that host task
executions (`ExecutionPlatform`), the node resource accounting
(`ResourceManager`) and the language mirrors. A task arrives as an
`Invocation` (contract from `api`), is wrapped into an
`InvocationExecutionRequest` and is picked by an `Executor` thread, which
reserves resources, builds a task sandbox and delegates the user code
execution to a language `Invoker` resolved through the `InvokerFactory` SPI
(implemented by the `plugins` submodule). Language runtimes that cannot live
in the same process are driven through `BindingInvoker` variants that
delegate the task to a language binding outside the JVM: the piped family
exchanges a command string with the external process over a
`CommandChannel`, while the persistent family calls the persistent C
binding through JNI.

## Component diagram

```mermaid
graph TB
    subgraph "Node facade"
        EM["ExecutionManager"]
    end

    subgraph "ExecutionPlatform (CPUThreadPool / IOThreadPool)"
        Q["JobQueue<br/>(ExecutorRequest)"]
        EX["Executor threads"]
        T["Deadline Timer"]
    end

    subgraph "Resource management"
        RM["ResourceManager"]
        TB["ThreadBinder<br/>BindToMap / BindToResource / Unbinded"]
    end

    subgraph "Invoker framework (SPI)"
        IF["InvokerFactory.Registry"]
        INV["Invoker (per language,<br/>from plugins)"]
        EINV["BindingInvoker<br/>piped / persistent / python-mpi"]
    end

    subgraph "Out-of-process executors"
        MM["MirrorMonitor"]
        M["Mirror<br/> PipedMirror / PersistentMirror"]
        BE["BindingExecutor<br/> PipedExecutor / PersistentExecutor"]
        CC["PipePair<br/>(CommandChannel)"]
    end

    EM -->|"enqueue(invocation)"| Q
    Q --> EX
    EX -->|"acquireResources / releaseResources"| RM
    RM --> TB
    EX -->|"limitExecution"| T
    EX -->|"getInvoker"| IF
    IF --> INV
    INV --> EINV
    EINV -->|"task command"| BE
    BE -->|"sendCommand / readCommand"| CC
    MM --> |"watches"| M
    MM --> |"watches"| BE
    M --- BE
    EM -->|"startMirror(methodType, lang)"| M
    INV -->|"nested tasks"| RT["COMPSsRuntime<br/>(compss-runtime-api)"]
```

## Interfaces and implementations

The class diagram below shows how the main contracts of the module
relate to their implementations.

```mermaid
classDiagram
    direction LR

    class InvocationContext {
        <<interface>>
    }
        
    namespace EM["ExecutionManager"] {

        class ExecutionManager {

        }

        namespace EP["ExecutionPlatform"] {

            class ExecutorContext {
                <<interface>>
            }

            class InvocationRunner {
                <<interface>>
            }

            class ExecutionPlatform

            class Executor        
            
            class MirrorFactory~M~ {
                <<interface>>
            }


            namespace M["Mirrors"] {
                class ExecutorContextMirror~E~ {
                    <<interface>>
                }

                class PipedMirror {
                    <<abstract>>
                }

                class PersistentMirror

                class ExternalExecutor {
                    <<abstract>>
                }

                class PipedExecutor

                class PersistentExecutor
            }
        }

    }
    namespace I["Invokers"] {
        class InvokerFactory {
            <<interface>>
        }

        class Invoker~W~ {
            <<abstract>>
        }

        class BindingInvoker {
            <<abstract>>
        }

        class PipedInvoker {
            <<abstract>>
        }

        class PersistentInvoker {
            <<abstract>>
        }

    }



    class CommandChannel~T~ {
        <<interface>>
    }

    class PipePair



    %% Platform contract and its implementation
    ExecutionManager o-- ExecutionPlatform : handles


    ExecutorContext <|.. ExecutionPlatform
    ExecutorContext ..> InvocationContext
    Executor --> ExecutorContext : belongs to
    ExecutionPlatform o-- Executor : gathers
    ExecutionPlatform o-- ExecutorContextMirror : mirrored as
    ExecutionPlatform ..> MirrorFactory: uses
    MirrorFactory ..> ExecutorContextMirror : creates

    %% Mirrors: the platform seen from an external process
    ExecutorContextMirror <|.. PipedMirror
    ExecutorContextMirror <|.. PersistentMirror
    PipedMirror --> ExecutorContext : mirrored context
    ExecutorContextMirror ..> ExternalExecutor : getMirroredExecutor
    PersistentMirror o-- PersistentExecutor
    PipedMirror o-- PipedExecutor
    PipedExecutor --> PipePair : pipePair
    
    %% Command protocol towards external processes
    CommandChannel <|.. PipePair

    %% Executor threads and invokers
    InvocationRunner <|.. Executor
    Invoker ..> InvocationRunner : runner
    Invoker ..> InvocationContext : context
    Invoker <|-- BindingInvoker
    BindingInvoker <|-- PipedInvoker
    BindingInvoker <|-- PersistentInvoker

    %% Factory SPI
    Executor ..> InvokerFactory : Registry.getInvoker
    InvokerFactory ..> Invoker : build
```

- **`InvocationContext`** (contract from `api`): everything an
  invocation needs from its environment: node configuration, thread
  streams, parameter load/store operations and the runtime API entry
  point for nested tasks. It is implemented outside the execution
  module by the worker adaptors (`NIOWorker`, `GOSWorker`,
  `COMPSsMaster`); `test-commons` provides `FakeInvocationContext`.
- **`ExecutorContext`** (core): the execution platform as seen by
  executors, invoker factories and mirrors: request dispatch
  (`newThread`/`getJob`), resource management, running-job registry
  (deadline timer), the mirror registry
  (`getMirror`/`registerMirror`) and the mirrored-executor lookup
  (`getMirroredExecutor`: the synchronized find-or-create of the mirror
  through its `MirrorFactory`, then the mirror's thread-local executor).
  `ExecutionPlatform` is its only implementation in the core, hosting the
  thread pools, the `JobQueue`, the `ResourceManager` and the deadline
  `Timer`.
- **`Executor`**: a worker thread of the platform. It implements
  `InvocationRunner` so that invokers can stall it while a blocked
  nested workflow runs and wake it up afterwards.
- **`Invoker`**: abstract base of all language invokers. It implements
  `WorkflowListener` (workflow API) to track nested task groups.
  In-process invokers extend it directly (`JavaInvoker` in the java
  plugin, plus the binary and opencl plugins), while out-of-process
  execution goes through `BindingInvoker`, which delegates the task to a
  language binding outside the JVM: its constructor builds the task
  command in two steps (the subclass execution command through
  `getTaskExecutionCommand`, then the generic external arguments —
  affinities, hostlist, parameters — appended by the base class), and
  its tail unifies the exit-value check and the result application
  (`checkExitValue`/`applyTaskResults`). Its specializations are
  `PipedInvoker` (C, Python, R — exchanges the command with the piped
  process through its executor handle's `CommandChannel`),
  `PersistentInvoker` (persistent C — synchronous calls into the JNI
  binding) and `PythonMPIInvoker` (binary plugin).
- **`InvokerFactory`**: SPI implemented by every language plugin and
  discovered through `ServiceLoader`. `preLoad` starts and registers
  the language mirror on the platform and `build` creates the
  invoker, fetching its executor handle through the platform's
  mirrored-executor lookup. `CInvokerFactory` is shown as an example.
- **`ExecutorContextMirror`**: mirrors an execution platform into
  external processes. `registerExecutor(Executor)` returns the handle
  (`E`) used to command the process of that executor;
  `getMirroredExecutor(Executor)` is the lazy thread-local lookup that
  re-registers a handle when the previous one can no longer run, and
  `cancelJob(E)` cancels through the same handle type, keeping the
  contract transport-agnostic. Each mirror owns its executor registry
  and its per-thread handle cache: `PipedMirror` (pipe-based, holding
  the pipe pool and specialized per language by `CMirror`,
  `PythonMirror` and `RMirror`) and `PersistentMirror` (persistent
  backends, managing the thread-binding lifecycle). The command
  protocol is `CommandChannel` (`sendCommand`/`readCommand`),
  implemented by `PipePair`, which is what `PipedInvoker` and
  `PipedExecutor` exchange commands through.
- **`ExternalExecutor`**: abstract base of the runtime-side handles to
  out-of-process executors. It owns the handle lifecycle as a state
  machine (CREATING, RUNNING, STOPPING, DELETED with final transition
  methods), so every handle follows the same enforced contract:
  `PipedExecutor` adds its mirror and pipe pair, `PersistentExecutor`
  adds its owning executor thread. The persistent JNI bridge
  (`PersistentCBinding`) owns the native entry points of the persistent
  C binding: the invoker calls `executeInBinding` with the task command
  string, and the mirror initializes and finishes the thread binding on
  executor registration and unregistration.

## Execution flow of an invocation

```mermaid
sequenceDiagram
    actor User

    create participant Invocation
    User->>Invocation: create
    create participant L as InvocationExecutionListener
    User->>L: create


    participant EM as ExecutionManager
    participant P as ExecutionPlatform
    participant E as Executor thread
    participant RM as ResourceManager
    participant F as InvokerFactory.Registry
    participant I as Invoker (plugin)

    User->>+EM: enqueue(invocation, listener)
    EM->>+P: execute(InvocationExecutionRequest)
    P->>P: queue.enqueue(request)
    P-->>-EM: 
    EM-->>-User: 
    E->>+P: getRequest() [blocking]
    P-->>-E: request
    loop While request is not StopExecutorRequest
        E->>+E: processInvocation(inv, listener)
        E->>Invocation: executionStarts()
        E->>+RM: acquireResources(jobId, requirements)
        Note over RM: assigns CPUs/GPUs bound via ThreadBinder
        RM-->>-E: coreIds
        E->>+E: create task sandbox
        E->>+E: redirect thread streams
        E->>+F: getInvoker
        F-->>-E: invoker (first factory accepting the invocation)
        E->>+P: registerRunningJob(invocation, invoker)
        Note over P: deadline timer armed 
        P->>-E:
        E->>+I: invoker.runInvocation(this)
        Note over I: process parameters, set environment,<br/>run prolog, invokeMethod(), run epilog
        I-->>-E: results (serialized into result parameters)
        E->>+P: unregisterRunningJob(jobId)
        Note over P: cancelled deadline timer
        P->>-E:
        loop for each param
            E->>L: onResultAvailable(param) (per result parameter, during stage-out)
        end
        E->>Invocation: executionEnds
        E->>+RM: releaseResources(jobId)
            Note over RM: releases CPUs/GPUs bound via ThreadBinder
        RM->>-E:
        E->>-E: remove task sandbox
        E->>-E: redirect thread streams
        E->>L: notifyEnd(invocation, success, exception)
        E->>+P: getRequest() [blocking]
        P-->>-E: request
    end
```

Activation bars in the diagram show the interval during which each
component is busy processing a call. The submission chain
(`ExecutionManager` and `ExecutionPlatform`) is only active until the
request is queued, while the executor thread remains active for the
whole processing of each request.

Step by step:

1. **Submission.** The caller first builds the task description
   (`Invocation`) and the listener that will receive its completion
   callbacks (`InvocationExecutionListener`), and then passes both to
   `ExecutionManager.enqueue`, which wraps them into an
   `InvocationExecutionRequest`. IO-bound executions (e.g. object streams)
   are routed to the dedicated `IOThreadPool`, the rest to the
   `CPUThreadPool`. The whole submission chain is asynchronous: the platform
   only inserts the request into its `JobQueue` and both
   `ExecutionPlatform.execute` and `ExecutionManager.enqueue` return
   immediately, without waiting for the job to end.
2. **Dispatch.** Each `ExecutionPlatform` keeps a `JobQueue` of pending
   requests. `Executor` threads pull them: the thread registers itself with
   `newThread()` and then loops over `getJob()`, which blocks until a
   request arrives (or a stop request terminates the loop with a
   `StopExecutorException`).
3. **Resource reservation.** Before running, the executor acquires the
   resources described by `invocation.getRequirements()` from the
   `ResourceManager`, which assigns concrete CPUs/GPUs and optionally binds
   the executor thread through the configured `ThreadBinder`. If resources
   are exhausted, the request blocks in the manager's pending requests until
   a release wakes it up.
4. **Sandbox and streams.** The executor creates the task working directory
   (sandbox), registers the job outputs and redirects the thread's
   stdout/stderr (`ThreadedPrintStream` and per-thread `System` properties
   via `ThreadedProperties`) so that concurrent tasks on the same node write
   to their own files.
5. **Invoker resolution.** `InvokerFactory.Registry.getInvoker` asks every
   factory supporting the invocation's `MethodType` to build an invoker; the
   first factory whose language matches wins. This is the extension point
   that decouples the engine from the language plugins.
6. **Execution.** The executor registers the running job (arming the
   platform deadline timer with the invocation timeout) and calls
   `invoker.runInvocation(this)`. The base `Invoker` parses the parameters,
   sets the execution environment variables, runs the prolog, calls the
   plugin's `invokeMethod()`, and runs the epilog. Task tracing and data
   provenance events are emitted around the method invocation.
7. **Release and notification.** The executor unregisters the job and
   releases the resources back to the `ResourceManager`. During the
   stage-out of the result parameters it reports every result through
   `InvocationExecutionListener.onResultAvailable(param)`, and once the
   invocation ends (`invocation.executionEnds()`) the request itself reports
   the job outcome with `notifyEnd(invocation, success, exception)`:
   success, the `WorkflowException` of a failed task group, or the failure
   flag of any other error. On timeout, the platform timer cancels the
   invoker (`invoker.cancel()`).

## Resource handling

The `ResourceManager` is the single owner of the node capabilities (CPU,
GPU, FPGA and other computing units). `acquireResources` assigns units per
job id, honoring the resource description of each invocation and the
configured binding map; `releaseResources` returns them. When an invocation
blocks on a nested-task synchronization, the platform may release its
resources for other tasks and re-acquire them when the code is ready to
continue (`Executor.stalledCodeExecution` / `readyToContinueExecution`,
controlled by the `reuseResourcesOnBlockedInvocation` platform setting and
implemented through the `Invoker` `WorkflowListener` callbacks).

Thread affinity is applied by the `ThreadBinder` implementation selected at
startup: `BindToMap` (fixed map from `lscpu` or user configuration),
`BindToResource` (cores of the hosting resource) or `Unbinded` (no binding).

## Out-of-process executors

Languages whose runtime cannot be embedded in the Java worker (Python, C)
are executed in external processes:

- **Piped** (`PipedInvoker` / `PipedExecutor`): the `PipedMirror` starts a
  piper process (the `bindings_piper.sh` script of the NIO adaptor) and
  creates one `PipedExecutor` per task, connected through a `PipePair`.
  The invoker owns the executor handle, derives its command channel
  from it and serializes the execution request into an
  `ExecuteTaskExternalCommand` built by the `BindingInvoker` constructor;
  it reads back task events over the same protocol; all commands are the
  `ExternalCommand` implementations of the `executor.external.commands`
  package (task execution and cancellation, task groups, files, objects,
  barriers, channels, worker lifecycle).
  The `MirrorMonitor` watches the mirrored elements (files, objects, task
  groups) so the worker can react to their lifecycle.
- **Persistent** (`PersistentInvoker` / `PersistentExecutor`): the external
  executor is a long-lived process whose state survives across
  invocations, avoiding the per-task startup cost. It does not exchange
  commands over a channel: the invoker hands the same command string to
  the persistent C binding through JNI (`PersistentCBinding
  .executeInBinding`), which executes it on the binding of the calling
  executor thread and reports back an `ExternalTaskStatus` that drives
  the shared result handling. The mirror manages the thread-binding
  lifecycle, initializing the binding when an executor thread registers
  and finishing it on unregistration; since jobs run as synchronous
  native calls on the executor thread, there is nothing to cancel from
  the outside.

The platform starts a mirror per (method type, language) pair on demand:
`ExecutionManager.startMirror` triggers `InvokerFactory.Registry.preLoad`,
which lets the matching plugin boot its language infrastructure.

## Nested tasks

An invocation may detect more parallelism than declared (e.g. a Python task
that spawns other tasks through the COMPSs API). The invoker then registers
a workflow with the runtime API obtained from the context
(`context.getRuntimeAPI()`) and becomes a *nested application*: input files
are registered in the workflow, the invocation waits on `wf.barrier()` for
all its nested tasks, and the results bound by the nested tasks are
forwarded to the outer invocation parameters (`resultIsForwarded`). While
the nested workflow runs, the invoker releases and re-acquires the hosting
thread's resources through the `InvocationRunner` callbacks described
above.
