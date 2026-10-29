# compss-execution-test-commons

Technical documentation of the `test-commons` submodule of
[compss-execution](../README.md).

## Purpose

`compss-execution-test-commons` provides the shared test infrastructure used
to verify invokers and the execution engine without deploying a real COMPSs
runtime. It offers fake implementations of the invocation contracts defined
in `api` plus a verifier that checks the expected interaction flow between
an invoker and its context.

## Responsibilities

- Provide fake, builder-style implementations of the `Invocation`,
  `InvocationParam` and `InvocationContext` contracts.
- Provide the `InvokerListener` interface that a test uses to observe and
  react to the execution performed by an invoker.
- Provide the `ExecutionFlowVerifier`, which records the expected sequence of
  context/invoker interactions and asserts that the actual execution matches
  it.
- Provide simple test objects, including one backed by the storage API, to
  play the role of task parameter values.

## Functionalities

### Package `es.bsc.compss.invokers.test.utils` — fakes and verifier

- `FakeInvocation`: a configurable `Invocation` implementation for tests
  (job/task identifiers, task type, method implementation, requirements,
  parameters, target, results and slave nodes).
- `FakeInvocationParam`: a configurable `InvocationParam` implementation
  (type, prefix, name, content type, STD IO stream, value and value class,
  renaming information, and the rest of the contract accessors).
- `FakeInvocationContext`: an `InvocationContext` implementation with
  listener hooks (`InvocationContextListener`). Instead of managing real
  data, it delegates the interesting operations (object retrieval, object
  storage, method execution) to the listener, which the test controls.
- `ExecutionFlowVerifier`: implements both `InvocationContextListener` and
  `InvokerListener`. The test declares the expected events in order with
  `add(Event.Type, state, reply)`; as the invoker runs, every interaction
  (loading a parameter, storing a value, running the method, returning
  results) polls the next event and asserts its type, applies the
  parameter assertions attached to it, and produces the scripted replies
  (created objects, persistent objects, updated or persisted values).
  `testCompleted()` fails if any expected event is still pending.

### Package `es.bsc.compss.invokers.test.utils.types` — verifier vocabulary

- `Event`: an expected interaction, with a `Type`
  (`GET_OBJECT`, `GET_PERSISTENT_OBJECT`, `STORE_OBJECT`,
  `STORE_PERSISTENT_OBJECT`, `RUNNING_METHOD`, `METHOD_RETURN`), the
  expected state (parameter assertions) and the attached reply (parameter
  actions).
- `Role`: the role of a parameter in the invocation: `ARGUMENT`, `TARGET`
  or `RETURN`.
- `InvocationParameterAssertion`: a check to validate on a parameter of the
  invocation when its event happens.
- `InvocationParameterAction`: a scripted effect to apply when the event
  happens (create a return value, create a persistent object, update a
  value, persist an object), bound to a parameter obtained through its
  `Role`.

### Package `es.bsc.compss.invokers.test` — listener contract

- `InvokerListener`: `runningMethod(Invocation)`, called when the fake
  context would execute the user method (returns the method results), and
  `methodReturn(List<Object>)`, called when the method completes.

### Package `es.bsc.compss.invokers.test.objects` — test data

- `TestObject`: simple comparable object with an integer value and an
  `updateValue` method, used as a plain task parameter value.
- `StorageTestObject`: the same idea backed by a storage stub
  (`storage.StubItf`), used to exercise persistent object handling.

### Usage by the plugin tests

The plugin modules depend on `compss-execution-test-commons` to test their
invokers in isolation: each test builds a `FakeInvocation` describing a task,
wraps an `ExecutionFlowVerifier` in a `FakeInvocationContext`, builds its
invoker through its factory, runs the invocation and lets the verifier
assert the whole interaction flow (for example
`compss-execution-java-plugin`'s `TestJavaInvoker`).

## Dependencies

- [compss-execution-api](../api/README.md): the contracts being faked
  (`Invocation`, `InvocationParam`, `InvocationContext`).
- [compss-commons](https://gitlab.bsc.es/wdc/compss/framework/-/tree/trunk/compss/runtime/commons):
  semantic types, method implementations and resource descriptions used by
  the fake invocation.
- [compss-storage](https://gitlab.bsc.es/wdc/compss/framework/-/tree/trunk/compss/runtime/storage):
  storage API stub, used by `StorageTestObject`.
- JUnit 4: assertions used by the `ExecutionFlowVerifier`.
