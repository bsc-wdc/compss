/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.invokers.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.test.objects.StorageTestObject;
import es.bsc.compss.invokers.test.objects.TestObject;
import es.bsc.compss.invokers.test.utils.ExecutionFlowVerifier;
import es.bsc.compss.invokers.test.utils.FakeInvocation;
import es.bsc.compss.invokers.test.utils.FakeInvocationContext;
import es.bsc.compss.invokers.test.utils.FakeInvocationParam;
import es.bsc.compss.invokers.test.utils.FakeInvoker;
import es.bsc.compss.invokers.test.utils.types.Event;
import es.bsc.compss.invokers.test.utils.types.InvocationParameterAction;
import es.bsc.compss.invokers.test.utils.types.InvocationParameterAction.Action;
import es.bsc.compss.invokers.test.utils.types.InvocationParameterAssertion;
import es.bsc.compss.invokers.test.utils.types.InvocationParameterAssertion.Field;
import es.bsc.compss.invokers.test.utils.types.Role;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.InvalidMapException;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.worker.COMPSsException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.UUID;

import org.junit.Test;


public class TestInvoker {

    private final ExecutionFlowVerifier expectedEvents = new ExecutionFlowVerifier();


    private static ExecutionSandbox createTempDirectory() throws IOException {
        Path tempPath = Files.createTempDirectory("temp");
        File temp = tempPath.toFile();
        temp.deleteOnExit();
        return new ExecutionSandbox(temp, true);
    }

    public AbstractMethodImplementation genDummy(String className, String methodName, Integer coreId, Integer implId,
        String signature, MethodResourceDescription mrd) {
        return new AbstractMethodImplementation(coreId, implId, new ImplementationDescription<>(
            new MethodDefinition(className, methodName), signature, false, mrd, null, null));
    }

    @Test
    public void emptyInvoker() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr.setListener(expectedEvents);
        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        actions.add(new InvocationParameterAction(Role.RETURN, 0, Action.CREATE, 3));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedResults.add(new TestObject(3));
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        checkInvocation(invocation, new InvocationParam[] {}, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void booleanInputParameter()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        InvocationParam p = new FakeInvocationParam(DataType.BOOLEAN_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", "", false);
        p.setValue(true);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, true));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, boolean.class));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        Invocation invocation = invBr.build();
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.BOOLEAN_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", "", false);
        endParam0.setValue(true);
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void charInputParameter() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        InvocationParam p = new FakeInvocationParam(DataType.CHAR_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", "", false);
        p.setValue('a');
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, 'a'));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, char.class));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.CHAR_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", "", false);
        endParam0.setValue('a');
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void byteInputParameter() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        InvocationParam p = new FakeInvocationParam(DataType.BYTE_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", "", false);
        p.setValue(240);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, 240));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, byte.class));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.BYTE_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", "", false);
        endParam0.setValue(240);
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void shortInputParameter() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        InvocationParam p = new FakeInvocationParam(DataType.SHORT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", "", false);
        p.setValue(25);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, 25));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, short.class));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.SHORT_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", "", false);
        endParam0.setValue(25);
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void integerInputParameter()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        InvocationParam p =
            new FakeInvocationParam(DataType.INT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", "", false);
        p.setValue(878544);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, 878544));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, int.class));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 =
            new FakeInvocationParam(DataType.INT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", "", false);
        endParam0.setValue(878544);
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void longInputParameter() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        InvocationParam p = new FakeInvocationParam(DataType.LONG_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", "", false);
        p.setValue(832478544);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, 832478544));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, long.class));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.LONG_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", "", false);
        endParam0.setValue(832478544);
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void floatInputParameter() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        InvocationParam p = new FakeInvocationParam(DataType.FLOAT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", "", false);
        p.setValue(832.23f);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, 832.23f));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, float.class));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.FLOAT_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", "", false);
        endParam0.setValue(832.23f);
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void doubleInputParameter() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        InvocationParam p = new FakeInvocationParam(DataType.DOUBLE_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", "", false);
        p.setValue(83.31415644d);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, 83.31415644d));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, double.class));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.DOUBLE_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", "", false);
        endParam0.setValue(83.31415644d);
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void stringInputParameter() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        InvocationParam p = new FakeInvocationParam(DataType.STRING_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", "", false);
        p.setValue("Test String");
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, "Test String"));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, String.class));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.STRING_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", "", false);
        endParam0.setValue("Test String");
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void multiplePrimitiveInputParameter()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        LinkedList<InvocationParam> params = new LinkedList<>();
        InvocationParam p = new FakeInvocationParam(DataType.DOUBLE_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", "", false);
        p.setValue(83.31415644d);
        params.add(p);
        p = new FakeInvocationParam(DataType.STRING_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", "",
            false);
        p.setValue("Test String");
        params.add(p);
        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, 83.31415644d));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, double.class));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 1, Field.VALUE, "Test String"));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 1, Field.VALUE_CLASS, String.class));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 1, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.DOUBLE_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", "", false);
        endParam0.setValue(83.31415644d);
        InvocationParam endParam1 = new FakeInvocationParam(DataType.STRING_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", "", false);
        endParam1.setValue("Test String");
        checkInvocation(invocation, new InvocationParam[] { endParam0,
            endParam1 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void objectInputParameter() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        String renaming = "d5v3_754478989756456.IT";
        Object value = new TestObject(3);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, false);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming, value);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", renaming, false);
        endParam0.setValue(new TestObject(3));
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void objectInoutParameter() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        String renaming = "d5v3_754478989756456.IT";
        Object value = new TestObject(3);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, true);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming, value);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, true));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        actions.add(new InvocationParameterAction(Role.ARGUMENT, 0, Action.UPDATE, 5));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        // expectedEvents.add(Event.Type.STORE_OBJECT, renaming, 5);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", renaming, false);
        endParam0.setValue(new TestObject(5));
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void targetInWithEmptyParameter()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        String renaming = "d5v3_754478989756456.IT";
        Object value = new TestObject(3);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, false);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming, value);
        invBr = invBr.setTarget(p);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam target = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", renaming, false);
        target.setValue(new TestObject(3));
        checkInvocation(invocation, new InvocationParam[] {}, target, null);
        sandBoxDir.clean();
    }

    @Test
    public void targetInoutWithEmptyParameter()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        String renaming = "d5v3_754478989756456.IT";
        Object value = new TestObject(3);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, true);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming, value);
        invBr = invBr.setParams(params);
        invBr = invBr.setTarget(p);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.WRITE_FINAL, true));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        actions.add(new InvocationParameterAction(Role.TARGET, 0, Action.UPDATE, 5));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        // expectedEvents.add(Event.Type.STORE_OBJECT, renaming, 5);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam target = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", renaming, true);
        target.setValue(new TestObject(5));
        checkInvocation(invocation, new InvocationParam[] {}, target, null);
        sandBoxDir.clean();
    }

    @Test
    public void resultWithEmptyParameter()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        String renaming = "d5v3_754478989756456.IT";
        // Object value = new TestObject(3);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, true);
        invBr = invBr.setParams(params);
        LinkedList<InvocationParam> results = new LinkedList<>();
        results.add(p);
        invBr = invBr.setResult(results);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.RETURN, 0, Field.WRITE_FINAL, true));

        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        actions.add(new InvocationParameterAction(Role.RETURN, 0, Action.CREATE, 5));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedResults.add(new TestObject(5));
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        // expectedEvents.add(Event.Type.STORE_OBJECT, renaming, 5);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();

        InvocationParam result = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", renaming, true);
        result.setValue(new TestObject(5));
        checkInvocation(invocation, new InvocationParam[] {}, null, new InvocationParam[] { result });
        sandBoxDir.clean();
    }

    @Test
    public void pSCOInputParameter() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        String id = UUID.randomUUID().toString();
        Object value = new StorageTestObject(id, 3);
        InvocationParam p = new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", id, false);
        expectedEvents.add(Event.Type.GET_PERSISTENT_OBJECT, id, value);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", id, false);
        endParam0.setValue(new StorageTestObject(id, 3));
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void persistedObjectInputParameter()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        LinkedList<InvocationParam> params = new LinkedList<>();
        String id = UUID.randomUUID().toString();
        Object value = new StorageTestObject(id, 3);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", id, false);
        expectedEvents.add(Event.Type.GET_OBJECT, id, null);
        expectedEvents.add(Event.Type.GET_PERSISTENT_OBJECT, id, value);
        params.add(p);
        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 = new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED,
            1.0, false, "", id, false);
        endParam0.setValue(new StorageTestObject(id, 3));
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void pSCOInoutParameter() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        String id = UUID.randomUUID().toString();
        Object value = new StorageTestObject(id, 3);
        InvocationParam p =
            new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", id, true);
        expectedEvents.add(Event.Type.GET_PERSISTENT_OBJECT, id, value);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, true));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        actions.add(new InvocationParameterAction(Role.ARGUMENT, 0, Action.UPDATE, 5));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        // expectedEvents.add(Event.Type.STORE_PERSISTENT_OBJECT, id, 5);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 =
            new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", id, true);
        endParam0.setValue(new StorageTestObject(id, 5));
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void persistedObjectInoutParameter()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        LinkedList<InvocationParam> params = new LinkedList<>();
        String id = UUID.randomUUID().toString();
        Object value = new StorageTestObject(id, 3);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", id, true);
        expectedEvents.add(Event.Type.GET_OBJECT, id, null);
        expectedEvents.add(Event.Type.GET_PERSISTENT_OBJECT, id, value);
        params.add(p);
        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, true));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        actions.add(new InvocationParameterAction(Role.ARGUMENT, 0, Action.UPDATE, 5));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        // expectedEvents.add(Event.Type.STORE_PERSISTENT_OBJECT, id, 5);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 =
            new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", id, true);
        endParam0.setValue(new StorageTestObject(id, 5));
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void persistingObjectInoutParameter()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {
        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();

        String renaming = "d5v3_754478989756456.IT";
        Object value = new StorageTestObject(3);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, true);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming, value);
        params.add(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.ARGUMENT, 0, Field.WRITE_FINAL, true));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        String id = UUID.randomUUID().toString();
        actions.add(new InvocationParameterAction(Role.ARGUMENT, 0, Action.PERSIST, id));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        // expectedEvents.add(Event.Type.STORE_PERSISTENT_OBJECT, id, 3);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam endParam0 =
            new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", id, true);
        endParam0.setValue(new StorageTestObject(id, 3));
        checkInvocation(invocation, new InvocationParam[] { endParam0 }, null, null);
        sandBoxDir.clean();
    }

    @Test
    public void pSCOInputTarget() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        String id = UUID.randomUUID().toString();
        Object value = new StorageTestObject(id, 3);
        InvocationParam p = new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", id, false);
        expectedEvents.add(Event.Type.GET_PERSISTENT_OBJECT, id, value);
        invBr = invBr.setParams(params);
        invBr = invBr.setTarget(p);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam target = new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", id, false);
        target.setValue(new StorageTestObject(id, 3));
        checkInvocation(invocation, new InvocationParam[] {}, target, null);
        sandBoxDir.clean();
    }

    @Test
    public void persistedObjectInputTarget()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        String id = UUID.randomUUID().toString();
        Object value = new StorageTestObject(id, 3);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", id, false);
        expectedEvents.add(Event.Type.GET_OBJECT, id, null);
        expectedEvents.add(Event.Type.GET_PERSISTENT_OBJECT, id, value);
        invBr = invBr.setTarget(p);
        LinkedList<InvocationParam> params = new LinkedList<>();
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.WRITE_FINAL, false));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam target = new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", id, false);
        target.setValue(new StorageTestObject(id, 3));
        checkInvocation(invocation, new InvocationParam[] {}, target, null);
        sandBoxDir.clean();
    }

    @Test
    public void pSCOInoutTarget() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        String id = UUID.randomUUID().toString();
        Object value = new StorageTestObject(id, 3);
        InvocationParam p =
            new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", id, true);
        expectedEvents.add(Event.Type.GET_PERSISTENT_OBJECT, id, value);
        invBr = invBr.setTarget(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.WRITE_FINAL, true));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        actions.add(new InvocationParameterAction(Role.TARGET, 0, Action.UPDATE, 5));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        // expectedEvents.add(Event.Type.STORE_PERSISTENT_OBJECT, id, 5);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam target =
            new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", id, true);
        target.setValue(new StorageTestObject(id, 5));
        checkInvocation(invocation, new InvocationParam[] {}, target, null);
        sandBoxDir.clean();
    }

    @Test
    public void persistedObjectInoutTarget()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {
        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        String id = UUID.randomUUID().toString();
        Object value = new StorageTestObject(id, 3);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", id, true);
        expectedEvents.add(Event.Type.GET_OBJECT, id, null);
        expectedEvents.add(Event.Type.GET_PERSISTENT_OBJECT, id, value);
        invBr = invBr.setTarget(p);
        LinkedList<InvocationParam> params = new LinkedList<>();
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.WRITE_FINAL, true));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        actions.add(new InvocationParameterAction(Role.TARGET, 0, Action.UPDATE, 5));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        // expectedEvents.add(Event.Type.STORE_PERSISTENT_OBJECT, id, 5);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam target =
            new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", id, true);
        target.setValue(new StorageTestObject(id, 5));
        checkInvocation(invocation, new InvocationParam[] {}, target, null);
        sandBoxDir.clean();
    }

    @Test
    public void persistingObjectInoutTarget()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        String renaming = "d5v3_754478989756456.IT";
        Object value = new StorageTestObject(3);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, true);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming, value);
        invBr = invBr.setTarget(p);
        invBr = invBr.setParams(params);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE, value));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.VALUE_CLASS, value.getClass()));
        assertions.add(new InvocationParameterAssertion(Role.TARGET, 0, Field.WRITE_FINAL, true));

        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        String id = UUID.randomUUID().toString();
        actions.add(new InvocationParameterAction(Role.TARGET, 0, Action.UPDATE, 5));
        actions.add(new InvocationParameterAction(Role.TARGET, 0, Action.PERSIST, id));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        // expectedEvents.add(Event.Type.STORE_PERSISTENT_OBJECT, id, 5);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam target =
            new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", id, true);
        target.setValue(new StorageTestObject(id, 5));
        checkInvocation(invocation, new InvocationParam[] {}, target, null);
        sandBoxDir.clean();
    }

    @Test
    public void persistedObjectResult()
        throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        String renaming = "d5v3_754478989756456.IT";
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, true);
        invBr = invBr.setParams(params);
        LinkedList<InvocationParam> results = new LinkedList<>();
        results.add(p);
        invBr = invBr.setResult(results);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.RETURN, 0, Field.WRITE_FINAL, true));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        String id = UUID.randomUUID().toString();
        actions.add(new InvocationParameterAction(Role.RETURN, 0, Action.CREATE_PERSISTENT, new Object[] { id,
            5 }));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedResults.add(new StorageTestObject(id, 5));
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        // expectedEvents.add(Event.Type.STORE_PERSISTENT_OBJECT, id, 5);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        invoker.runInvocation(null);

        expectedEvents.testCompleted();
        InvocationParam result =
            new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", id, true);
        result.setValue(new StorageTestObject(id, 5));
        checkInvocation(invocation, new InvocationParam[] {}, null, new InvocationParam[] { result });
        sandBoxDir.clean();
    }

    @Test
    public void persistedPscoResult() throws InvalidMapException, IOException, JobExecutionException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        LinkedList<InvocationParam> params = new LinkedList<>();
        String renaming = "d5v3_754478989756456.IT";
        InvocationParam p = new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, true);
        invBr = invBr.setParams(params);
        LinkedList<InvocationParam> results = new LinkedList<>();
        results.add(p);
        invBr = invBr.setResult(results);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod", 0, 0, "", new MethodResourceDescription()));

        LinkedList<InvocationParameterAssertion> assertions = new LinkedList<>();
        assertions.add(new InvocationParameterAssertion(Role.RETURN, 0, Field.WRITE_FINAL, true));
        LinkedList<InvocationParameterAction> actions = new LinkedList<>();
        String id = UUID.randomUUID().toString();
        actions.add(new InvocationParameterAction(Role.RETURN, 0, Action.CREATE_PERSISTENT, new Object[] { id,
            5 }));
        expectedEvents.add(Event.Type.RUNNING_METHOD, assertions, actions);
        LinkedList<TestObject> expectedResults = new LinkedList<>();
        expectedResults.add(new StorageTestObject(id, 5));
        expectedEvents.add(Event.Type.METHOD_RETURN, expectedResults, null);
        // expectedEvents.add(Event.Type.STORE_PERSISTENT_OBJECT, id, 5);

        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new FakeInvoker(context, invocation, sandBoxDir, null, expectedEvents);
        try {
            invoker.runInvocation(null);
        } catch (COMPSsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        expectedEvents.testCompleted();
        InvocationParam result =
            new FakeInvocationParam(DataType.PSCO_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", id, true);
        result.setValue(new StorageTestObject(id, 5));
        checkInvocation(invocation, new InvocationParam[] {}, null, new InvocationParam[] { result });
        sandBoxDir.clean();
    }

    private static void checkInvocation(Invocation invocation, InvocationParam[] params, InvocationParam target,
        InvocationParam[] results) {
        assertEquals("Invalid number of parameters at the end of the execution. " + "Obtained "
            + invocation.getParams().size() + " and only " + params.length + " expected.",
            invocation.getParams().size(), params.length);

        int parIdx = 0;
        for (InvocationParam param : invocation.getParams()) {
            assertEquals("Unexpected parameter type at the end of the execution. " + "Obtained " + param.getType()
                + " and " + params[parIdx].getType() + " expected.", param.getType(), params[parIdx].getType());

            assertEquals("Unexpected parameter value at the end of the execution. " + "Obtained " + param.getValue()
                + " and " + params[parIdx].getValue() + " expected.", param.getValue(), params[parIdx].getValue());
            parIdx++;
        }

        if (target == null && invocation.getTarget() != null) {
            fail("Not expecting a target parameter and one obtianed");
        }
        if (target != null && invocation.getTarget() == null) {
            fail("Expecting a target parameter and none obtianed");
        }
        if (target != null && invocation.getTarget() != null) {
            assertEquals("Unexpected target type at the end of the execution. " + "Obtained "
                + invocation.getTarget().getType() + " and " + target.getType() + " expected.",
                invocation.getTarget().getType(), target.getType());

            assertEquals(
                "Unexpected target value at the end of the execution. " + "Obtained "
                    + invocation.getTarget().getValue() + " and " + target.getValue() + " expected.",
                invocation.getTarget().getValue(), target.getValue());
        }

        assertEquals("Invalid number of results at the end of the execution. " + "Obtained "
            + invocation.getResults().size() + " and only 1 expected.", invocation.getParams().size(), params.length);

        int resIdx = 0;
        for (InvocationParam result : invocation.getResults()) {
            assertEquals("Unexpected parameter type at the end of the execution. " + "Obtained " + result.getType()
                + " and " + results[resIdx].getType() + " expected.", result.getType(), results[resIdx].getType());

            assertEquals("Unexpected parameter value at the end of the execution. " + "Obtained " + result.getValue()
                + " and " + results[resIdx].getValue() + " expected.", result.getValue(), results[resIdx].getValue());
            resIdx++;
        }
    }
}
