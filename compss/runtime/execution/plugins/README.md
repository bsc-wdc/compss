# compss-execution-plugins

Technical documentation of the `plugins` submodule of
[compss-execution](../README.md).

## Purpose

`compss-execution-plugins` provides the language-specific execution support
of compss-execution: one nested Maven module per programming language or
execution mechanism, each contributing invokers that run user task code on
the worker node. Plugins are not hardwired into the engine: they are
discovered at runtime through the Java Service Provider Interface (SPI),
which keeps the engine (`core`) independent of the supported languages.

## Responsibilities

- Implement the `InvokerFactory` SPI declared by `core`
  (`es.bsc.compss.invokers.InvokerFactory`).
- Provide one `Invoker` implementation per method type and language, on top
  of the base `Invoker<W>` class of `core`.
- Ship the SPI registration files that make the factories discoverable.

## Plugin discovery

Each plugin registers its factory class in
`src/main/resources/META-INF/services/es.bsc.compss.invokers.InvokerFactory`.
At runtime, `InvokerFactory.Registry` (in `core`) loads every registration
through `java.util.ServiceLoader` and indexes the factories by the
`MethodType`s they declare in `getSupportedTypes()`.

Two SPI rounds drive plugin usage:

- `InvokerFactory.Registry.preLoad(platform, methodType, language)`:
  prepares the infrastructure needed to host executions of a method type in
  a language (for example, starting the external executor process and its
  mirror).
- `InvokerFactory.Registry.getInvoker(executor, context, invocation, sandbox,
  resources)`: builds the invoker for a concrete invocation. Factories
  whose language does not match the invocation return `null` and the
  registry falls back to the next candidate; several plugins may declare
  support for the same `MethodType` (for example `METHOD`) as long as they
  answer for different languages.

## Nested plugins

| Plugin | Artifact | Supported method types | Invokers |
|--------|----------|------------------------|----------|
| `c` | `compss-execution-c-plugin` | `METHOD`, `MULTI_NODE` (C language) | `CInvoker`, `CPersistentInvoker` |
| `java` | `compss-execution-java-plugin` | `METHOD`, `MULTI_NODE` (Java language) | `JavaInvoker`, `JavaNestedInvoker`, `StorageInvoker` |
| `python` | `compss-execution-python-plugin` | `METHOD`, `MULTI_NODE` (Python language) | `PythonInvoker` (via piped external executor) |
| `r` | `compss-execution-r-plugin` | `METHOD`, `MULTI_NODE` (R language) | `RInvoker` |
| `binary` | `compss-execution-binary-plugin` | `BINARY`, `CONTAINER`, `MPI`, `PYTHON_MPI`, `MPMDMPI`, `OMPSS`, `DECAF`, `JULIA`, `COMPSs` | `binary` package: `BinaryInvoker`, `ContainerInvoker`, `MPIInvoker`, `MpmdMPIInvoker`, `PythonMPIInvoker`, `OmpSsInvoker`, `DecafInvoker`, `JuliaInvoker`, `RunCOMPSsInvoker` |
| `opencl` | `compss-execution-opencl-plugin` | `OPENCL` | `OpenCLInvoker` |
| `all` | `compss-execution-all-plugins` | (aggregator, none of its own) | Shaded jar aggregating all the plugins above |

Notes:

- The `java`, `python`, `c` and `r` plugins all serve the generic `METHOD`
  and `MULTI_NODE` types; they distinguish themselves by the language of the
  method implementation (`COMPSsConstants.Lang`), as shown above.
- The `binary` plugin covers every method type whose implementation is an
  external executable rather than in-process code: plain binaries, containers,
  MPI (single and MPMD, including Python MPI), OmpSs, Decaf, Julia and
  RunCOMPSs applications.
- `all` does not contain invokers: it packages all the other plugins into a
  single shaded jar (`maven-shade-plugin`) for deployments that want the
  complete language support with one artifact.

## How to add a new plugin

1. Create a nested module `plugins/<name>` with artifact
   `compss-execution-<name>-plugin`, parent `compss-execution-plugins`, and a
   dependency on `compss-execution-core` (for the `Invoker` base class and
   factory interface).
2. Implement `<Name>InvokerFactory` declaring the `MethodType`s it supports
   in `getSupportedTypes()`, the optional `preLoad` logic, and `build`
   returning an invoker when the invocation matches the plugin (or `null`
   otherwise).
3. Implement `<Name>Invoker` extending `Invoker<W>`, providing
   `invokeMethod()` and `cancelMethod()`.
4. Register the factory in
   `src/main/resources/META-INF/services/es.bsc.compss.invokers.InvokerFactory`.
5. Add the module to the `plugins/pom.xml` modules list and, if it must ship
   in the aggregated jar, as a dependency of `all/pom.xml`.

## Dependencies

- [compss-execution-core](../core/README.md): the `Invoker` base class,
  `InvokerFactory` SPI, `BinaryRunner` and the external executor protocol.
- [compss-execution-api](../api/README.md): invocation contracts
  (through core).
- The Python and C plugins additionally talk to their language bindings
  through piped external executors started by the plugin mirrors.
