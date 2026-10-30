# compss-execution

Technical documentation of the compss-execution module.

## Purpose

compss-execution is the task execution engine of the COMPSs runtime: it
hosts the execution of task invocations on the worker nodes of a COMPSs
deployment. The module receives task descriptions from the runtime, reserves
the node resources they need, and runs the user task code through pluggable
language invokers, while managing the worker-side state (files, objects,
task groups) that the executions produce.

## Context within the COMPSs Framework

compss-execution is part of the
[wdc/compss/framework](https://gitlab.bsc.es/wdc/compss/framework)
repository, at path `compss/runtime/execution`, and is aggregated by the
[compss-runtime](https://gitlab.bsc.es/wdc/compss/framework/-/tree/trunk/compss/runtime)
parent. It is consumed by the runtime communication adaptors that deploy and
drive the workers (for example the
[NIO adaptor](https://gitlab.bsc.es/wdc/compss/framework/-/tree/trunk/compss/runtime/adaptors/nio))
and relies on these framework siblings:

- [compss-commons](https://gitlab.bsc.es/wdc/compss/framework/-/tree/trunk/compss/runtime/commons):
  semantic types, method implementation definitions, resource descriptions,
  tracing integration and logging configuration.
- [compss-runtime-api](https://gitlab.bsc.es/wdc/compss/framework/-/tree/trunk/compss/runtime/api):
  the `COMPSsRuntime` API, used for nested task execution.
- [compss-storage](https://gitlab.bsc.es/wdc/compss/framework/-/tree/trunk/compss/runtime/storage):
  the storage API stubs used by persistent objects in tests and bindings.

## Architecture overview

The module is split into a contract module, an engine, a set of language
plugins and a shared test infrastructure:

- **api** declares the contracts consumed by every participant: the task
  execution request (`Invocation`), its parameters
  (`InvocationParam`), the execution environment (`InvocationContext`), the
  thread binding contract (`ThreadBinder`) and the exceptions and tracing
  events of the execution process.
- **core** implements the engine: the execution platforms hosting the
  executor threads, the resource management and thread binding, the invoker
  framework with its `InvokerFactory` SPI, and the command protocol used to
  drive out-of-process executors (piped and persistent).
- **plugins** provide the language-specific invokers (C, Java, Python, R,
  binary executables including MPI/containers/OmpSs/Decaf/Julia/RunCOMPSs,
  OpenCL and an aggregator jar), discovered at runtime through the Java
  SPI.
- **test-commons** offers fake contracts and an execution flow verifier used
  by the plugin tests.

The dependency direction is strictly `api` <- `core` <- `plugins` (and
`test-commons` building on `api`), so the engine never depends on a
concrete language and new languages can be added as separate plugins.

## Module summary

| Module | Responsibility | Documentation |
|--------|----------------|---------------|
| api | Shared contracts and types with no implementation | [api/README.md](api/README.md) |
| core | Execution engine: executor threads, resource management, invoker framework, out-of-process executor protocol | [core/README.md](core/README.md) |
| plugins | Language invoker plugins discovered via Java SPI | [plugins/README.md](plugins/README.md) |
| test-commons | Shared test infrastructure: fake contracts and execution flow verifier | [test-commons/README.md](test-commons/README.md) |

For a runtime-level description of the engine, see the core
[architecture document](core/docs/ARCHITECTURE.md).
