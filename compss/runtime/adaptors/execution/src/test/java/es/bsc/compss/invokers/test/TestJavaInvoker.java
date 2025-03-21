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
import es.bsc.compss.invokers.JavaInvoker;
import es.bsc.compss.invokers.test.objects.TestObject;
import es.bsc.compss.invokers.test.utils.ExecutionFlowVerifier;
import es.bsc.compss.invokers.test.utils.FakeInvocation;
import es.bsc.compss.invokers.test.utils.FakeInvocationContext;
import es.bsc.compss.invokers.test.utils.FakeInvocationParam;
import es.bsc.compss.invokers.test.utils.types.Event;
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
import java.util.HashMap;
import java.util.LinkedList;
import org.junit.Test;


public class TestJavaInvoker extends TestObject {

    public TestJavaInvoker() {
        super(0);
    }


    private static final String EXISTING_CLASS = "es.bsc.compss.invokers.test.TestJavaInvoker";
    private static final String NON_EXISTING_CLASS = "es.bsc.compss.invokers.test.NonExistentTestJavaInvoker";

    private static final String TEST_NONEXISTENT_METHODNAME = "nonExistent";
    private static final String TEST_EMPTY_METHODNAME = "testEmpty";
    private static final String TEST_READS_METHODNAME = "testReads";
    private static final String TEST_INOUT_METHODNAME = "testInouts";
    private static final String TEST_TARGET_IN_METHODNAME = "testTargetIn";
    private static final String TEST_TARGET_INOUT_METHODNAME = "testTargetInout";

    private static final String TEST_RESULT_METHODNAME = "testResult";

    private static final AbstractMethodImplementation TEST_EMPTY;
    private static final AbstractMethodImplementation TEST_READS;
    private static final AbstractMethodImplementation TEST_INOUT;
    private static final AbstractMethodImplementation TEST_TARGET_IN;
    private static final AbstractMethodImplementation TEST_TARGET_INOUT;
    private static final AbstractMethodImplementation TEST_RESULT;
    private static final AbstractMethodImplementation TEST_NONEXISTENT_CLASS;
    private static final AbstractMethodImplementation TEST_NONEXISTENT_METHOD;


    public static AbstractMethodImplementation genDummy(String className, String methodName, Integer coreId,
        Integer implId, String signature, MethodResourceDescription mrd) {
        return new AbstractMethodImplementation(coreId, implId, new ImplementationDescription<>(
            new MethodDefinition(className, methodName), signature, false, mrd, null, null));
    }


    static {
        TEST_EMPTY = genDummy(EXISTING_CLASS, TEST_EMPTY_METHODNAME, 0, 0, "", new MethodResourceDescription());
        TEST_READS = genDummy(EXISTING_CLASS, TEST_READS_METHODNAME, 0, 0, "", new MethodResourceDescription());
        TEST_INOUT = genDummy(EXISTING_CLASS, TEST_INOUT_METHODNAME, 0, 0, "", new MethodResourceDescription());
        TEST_TARGET_IN = genDummy(EXISTING_CLASS, TEST_TARGET_IN_METHODNAME, 0, 0, "", new MethodResourceDescription());
        TEST_TARGET_INOUT =
            genDummy(EXISTING_CLASS, TEST_TARGET_INOUT_METHODNAME, 0, 0, "", new MethodResourceDescription());
        TEST_RESULT = genDummy(EXISTING_CLASS, TEST_RESULT_METHODNAME, 0, 0, "", new MethodResourceDescription());

        TEST_NONEXISTENT_CLASS =
            genDummy(NON_EXISTING_CLASS, TEST_NONEXISTENT_METHODNAME, 0, 0, "", new MethodResourceDescription());
        TEST_NONEXISTENT_METHOD =
            genDummy(EXISTING_CLASS, TEST_NONEXISTENT_METHODNAME, 0, 0, "", new MethodResourceDescription());
    }

    private static HashMap<Long, ExecutionReport> executions = new HashMap<>();
    private final ExecutionFlowVerifier expectedEvents = new ExecutionFlowVerifier();


    private static ExecutionSandbox createTempDirectory() throws IOException {
        Path tempPath = Files.createTempDirectory("temp");
        File temp = tempPath.toFile();
        temp.deleteOnExit();
        return new ExecutionSandbox(temp, true);
    }

    @Test
    public void nonExistentClassTest() throws InvalidMapException, IOException, JobExecutionException {

        ExecutionSandbox sandBoxDir = createTempDirectory();

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setImpl(TEST_NONEXISTENT_CLASS);
        Invocation invocation = invBr.build();

        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        try {
            new JavaInvoker(context, invocation, sandBoxDir, null);
        } catch (JobExecutionException jee) {
            if (jee.getMessage().compareTo(JavaInvoker.ERROR_CLASS_REFLECTION) != 0) {
                fail("Test should fail because class could not be found. Obtained error is " + jee.getMessage());
            }
        }
        sandBoxDir.clean();
    }

    @Test
    public void nonExistentMethodTest() throws InvalidMapException, IOException, JobExecutionException {

        ExecutionSandbox sandBoxDir = createTempDirectory();

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setImpl(TEST_NONEXISTENT_METHOD);
        Invocation invocation = invBr.build();

        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        try {
            new JavaInvoker(context, invocation, sandBoxDir, null);
        } catch (JobExecutionException jee) {
            if (jee.getMessage().compareTo(JavaInvoker.ERROR_METHOD_REFLECTION) != 0) {
                fail("Test should fail because method could not be found. Obtained error is " + jee.getMessage());
            }
        }
        sandBoxDir.clean();
    }

    @Test
    public void parameterMissmatch() throws InvalidMapException, IOException, JobExecutionException {
        ExecutionSandbox sandBoxDir = createTempDirectory();

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setImpl(TEST_EMPTY);
        LinkedList<InvocationParam> params = new LinkedList<>();
        String renaming1 = "d5v3_754478989756456.IT";
        Object value1 = new TestObject(3);
        InvocationParam p1 = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming1, false);
        params.add(p1);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming1, value1);
        String renaming2 = "d6v3_754478989756456.IT";
        Object value2 = new TestObject(2);
        InvocationParam p2 = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming2, false);
        params.add(p2);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming2, value2);
        invBr = invBr.setParams(params);
        Invocation invocation = invBr.build();

        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        try {
            new JavaInvoker(context, invocation, sandBoxDir, null);
        } catch (JobExecutionException jee) {
            if (jee.getMessage().compareTo(JavaInvoker.ERROR_METHOD_REFLECTION) != 0) {
                fail("Test should fail because method could not be found. Obtained error is " + jee.getMessage());
            }
        }
        sandBoxDir.clean();
    }

    @Test
    public void emptyTest() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {
        long executorId = Thread.currentThread().getId();

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setImpl(TEST_EMPTY);
        Invocation invocation = invBr.build();

        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();

        ExecutionReport result = new ExecutionReport(TEST_EMPTY_METHODNAME, false, new Object[0], null, null);
        executions.put(executorId, result);
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new JavaInvoker(context, invocation, sandBoxDir, null);
        invoker.runInvocation(null);

        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_EMPTY_METHODNAME, true, new Object[] {}, null, null);
        sandBoxDir.clean();

    }

    /**
     * Testing if empty.
     */
    public static void testEmpty() {
        long executorId = Thread.currentThread().getId();
        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_EMPTY_METHODNAME, false, new Object[0], null, null);

        // DO WHATEVER THE METHOD DOES
        ExecutionReport result = new ExecutionReport(TEST_EMPTY_METHODNAME, true, new Object[0], null, null);
        executions.put(executorId, result);
    }

    @Test
    public void readsTest() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setImpl(TEST_READS);
        LinkedList<InvocationParam> params = new LinkedList<>();
        String renaming1 = "d5v3_754478989756456.IT";
        Object value1 = new TestObject(3);
        InvocationParam p1 = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming1, false);
        params.add(p1);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming1, value1);
        String renaming2 = "d6v3_754478989756456.IT";
        Object value2 = new TestObject(2);
        InvocationParam p2 = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming2, false);
        params.add(p2);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming2, value2);
        invBr = invBr.setParams(params);
        Invocation invocation = invBr.build();

        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        long executorId = Thread.currentThread().getId();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new JavaInvoker(context, invocation, sandBoxDir, null);

        ExecutionReport result = new ExecutionReport(TEST_READS_METHODNAME, false, new Object[] { value1,
            value2 }, null, null);
        executions.put(executorId, result);
        invoker.runInvocation(null);

        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_READS_METHODNAME, true, new Object[] { value1,
            value2 }, null, null);
        sandBoxDir.clean();
    }

    /**
     * Test reads.
     *
     * @param a test result a
     * @param b test result b
     */
    public static void testReads(TestObject a, TestObject b) {
        long executorId = Thread.currentThread().getId();
        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_READS_METHODNAME, false, new Object[] { a,
            b }, null, null);

        // DO WHATEVER THE METHOD DOES
        ExecutionReport result = new ExecutionReport(TEST_READS_METHODNAME, true, new Object[] { a,
            b }, null, null);
        executions.put(executorId, result);
    }

    @Test
    public void inoutsTest() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setImpl(TEST_INOUT);
        LinkedList<InvocationParam> params = new LinkedList<>();
        String renaming1 = "d5v3_754478989756456.IT";
        Object value1 = new TestObject(3);
        InvocationParam p1 = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming1, true);
        params.add(p1);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming1, value1);
        String renaming2 = "d6v3_754478989756456.IT";
        Object value2 = new TestObject(2);
        InvocationParam p2 = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming2, true);
        params.add(p2);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming2, value2);
        invBr = invBr.setParams(params);

        ExecutionReport result = new ExecutionReport(TEST_INOUT_METHODNAME, false, new Object[] { value1,
            value2 }, null, null);
        long executorId = Thread.currentThread().getId();
        executions.put(executorId, result);
        TestObject out1 = new TestObject(4);
        TestObject out2 = new TestObject(4);
        expectedEvents.add(Event.Type.STORE_OBJECT, renaming1, out1.getValue());
        expectedEvents.add(Event.Type.STORE_OBJECT, renaming2, out2.getValue());
        Invocation invocation = invBr.build();
        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new JavaInvoker(context, invocation, sandBoxDir, null);
        invoker.runInvocation(null);

        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_INOUT_METHODNAME, true, new Object[] { out1,
            out2 }, null, null);
        sandBoxDir.clean();
    }

    /**
     * Test inouts.
     *
     * @param a test result a
     * @param b test result b
     */
    public static void testInouts(TestObject a, TestObject b) {
        long executorId = Thread.currentThread().getId();
        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_INOUT_METHODNAME, false, new Object[] { a,
            b }, null, null);
        a.updateValue(a.getValue() + 1);
        b.updateValue(b.getValue() + 2);
        // DO WHATEVER THE METHOD DOES
        ExecutionReport result = new ExecutionReport(TEST_INOUT_METHODNAME, true, new Object[] { a,
            b }, null, null);
        executions.put(executorId, result);
    }

    @Test
    public void nullTargetTest() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {
        ExecutionSandbox sandBoxDir = createTempDirectory();

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setImpl(TEST_TARGET_IN);
        Invocation invocation = invBr.build();

        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();

        Invoker invoker = new JavaInvoker(context, invocation, sandBoxDir, null);
        try {
            invoker.runInvocation(null);
        } catch (NullPointerException npe) {
            // Executing a instance method on null -> Raise Exception
        }
        sandBoxDir.clean();
    }

    @Test
    public void targetInTest() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setImpl(TEST_TARGET_IN);
        LinkedList<InvocationParam> params = new LinkedList<>();
        invBr = invBr.setParams(params);
        String renaming = "d5v3_754478989756456.IT";
        TestObject target = new TestJavaInvoker();
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, false);
        invBr = invBr.setTarget(p);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming, target);
        Invocation invocation = invBr.build();

        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        long executorId = Thread.currentThread().getId();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new JavaInvoker(context, invocation, sandBoxDir, null);

        ExecutionReport result = new ExecutionReport(TEST_TARGET_IN_METHODNAME, false, new Object[] {}, target, null);
        executions.put(executorId, result);

        invoker.runInvocation(null);

        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_TARGET_IN_METHODNAME, true, new Object[] {}, target, null);
        sandBoxDir.clean();

    }

    /**
     * Test target in.
     */
    public void testTargetIn() {
        long executorId = Thread.currentThread().getId();
        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_TARGET_IN_METHODNAME, false, new Object[] {}, this, null);

        // DO WHATEVER THE METHOD DOES
        ExecutionReport result = new ExecutionReport(TEST_TARGET_IN_METHODNAME, true, new Object[] {}, this, null);
        executions.put(executorId, result);
    }

    @Test
    public void targetInoutTest() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setImpl(TEST_TARGET_INOUT);
        LinkedList<InvocationParam> params = new LinkedList<>();
        invBr = invBr.setParams(params);
        String renaming = "d5v3_754478989756456.IT";
        TestObject target = new TestJavaInvoker();
        target.updateValue(4);
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, true);
        invBr = invBr.setTarget(p);
        expectedEvents.add(Event.Type.GET_OBJECT, renaming, target);
        Invocation invocation = invBr.build();

        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        long executorId = Thread.currentThread().getId();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new JavaInvoker(context, invocation, sandBoxDir, null);

        ExecutionReport result =
            new ExecutionReport(TEST_TARGET_INOUT_METHODNAME, false, new Object[] {}, target, null);
        executions.put(executorId, result);

        expectedEvents.add(Event.Type.STORE_OBJECT, renaming, 5);
        invoker.runInvocation(null);
        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_TARGET_INOUT_METHODNAME, true, new Object[] {}, target, null);
        sandBoxDir.clean();
    }

    /**
     * Testing targetInout.
     */
    public void testTargetInout() {
        long executorId = Thread.currentThread().getId();
        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_TARGET_INOUT_METHODNAME, false, new Object[] {}, this, null);
        this.updateValue(this.getValue() + 1);
        // DO WHATEVER THE METHOD DOES
        ExecutionReport result = new ExecutionReport(TEST_TARGET_INOUT_METHODNAME, true, new Object[] {}, this, null);
        executions.put(executorId, result);
    }

    @Test
    public void resultTest() throws InvalidMapException, IOException, JobExecutionException, COMPSsException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setImpl(TEST_RESULT);
        LinkedList<InvocationParam> params = new LinkedList<>();
        invBr = invBr.setParams(params);
        String renaming = "d5v3_754478989756456.IT";
        InvocationParam p = new FakeInvocationParam(DataType.OBJECT_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0,
            false, "", renaming, true);
        LinkedList<InvocationParam> results = new LinkedList<>();
        results.add(p);
        invBr = invBr.setResult(results);
        Invocation invocation = invBr.build();

        FakeInvocationContext.Builder ctxBdr = new FakeInvocationContext.Builder();
        ctxBdr = ctxBdr.setListener(expectedEvents);
        FakeInvocationContext context = ctxBdr.build();
        long executorId = Thread.currentThread().getId();
        ExecutionSandbox sandBoxDir = createTempDirectory();
        Invoker invoker = new JavaInvoker(context, invocation, sandBoxDir, null);

        ExecutionReport result =
            new ExecutionReport(TEST_RESULT_METHODNAME, false, new Object[] {}, null, new TestObject(5));
        executions.put(executorId, result);

        expectedEvents.add(Event.Type.STORE_OBJECT, renaming, 5);
        invoker.runInvocation(null);

        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_RESULT_METHODNAME, true, new Object[] {}, null, null);
        sandBoxDir.clean();
    }

    /**
     * Testing result.
     * 
     * @return test result
     */
    public static TestObject testResult() {
        long executorId = Thread.currentThread().getId();
        ExecutionReport report = executions.remove(executorId);
        report.checkReport(TEST_RESULT_METHODNAME, false, new Object[] {}, null, null);

        TestObject returnValue = new TestObject(5);
        ExecutionReport result = new ExecutionReport(TEST_RESULT_METHODNAME, true, new Object[] {}, null, returnValue);
        executions.put(executorId, result);
        return returnValue;
    }


    private static class ExecutionReport {

        private final String method;
        private final boolean executed;
        private final Object[] arguments;
        private final Object target;
        private final Object result;


        public ExecutionReport(String method, boolean executed, Object[] arguments, Object target, Object result) {
            this.method = method;
            this.executed = executed;
            this.arguments = arguments;
            this.target = target;
            this.result = result;
        }

        public void checkReport(String method, boolean executed, Object[] arguments, Object target, Object result) {

            if (this.method.compareTo(method) != 0) {
                fail("Wrong method execution. Expecting" + method + " instead of " + this.method);
            }

            assertEquals("Unexpected execution state."
                + (executed ? "Obtained " + executed + " and " + this.executed + " expected."
                    : "Obtained " + this.executed + " and " + executed + " expected."),
                this.executed, executed);

            assertEquals(
                "The number of read does not match the expected. "
                    + (executed ? "Obtained " + arguments.length + " and " + this.arguments.length + " expected."
                        : "Obtained " + this.arguments.length + " and " + arguments.length + " expected."),
                this.arguments.length, arguments.length);

            for (int argIdx = 0; argIdx < arguments.length; argIdx++) {
                assertEquals(
                    "Unexpected value for parameter " + argIdx + " on " + method + "."
                        + (executed ? "Obtained " + arguments[argIdx] + " and " + this.arguments[argIdx] + " expected."
                            : "Obtained " + this.arguments[argIdx] + " and " + arguments[argIdx] + " expected."),
                    this.arguments[argIdx], arguments[argIdx]);
            }

            if (this.target == null && target == null) {
                assertEquals("Unexpected target value. "
                    + (executed ? "Obtained " + target + " and " + this.target + " expected."
                        : "Obtained " + this.target + " and " + target + " expected."),
                    this.target, target);
            }

            if (this.result == null && result == null) {
                assertEquals("Unexpected result value. "
                    + (executed ? "Obtained " + result + " and " + this.result + " expected."
                        : "Obtained " + this.result + " and " + result + " expected."),
                    this.result, result);
            }
        }
    }
}
