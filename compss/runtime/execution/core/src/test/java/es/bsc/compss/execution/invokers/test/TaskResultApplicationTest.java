/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

package es.bsc.compss.execution.invokers.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.execution.Invocation;
import es.bsc.compss.execution.InvocationContext;
import es.bsc.compss.execution.InvocationParam;
import es.bsc.compss.execution.InvocationParamCollection;
import es.bsc.compss.execution.exceptions.JobExecutionException;
import es.bsc.compss.execution.executor.ExecutionSandbox;
import es.bsc.compss.execution.executor.binding.commands.ExecuteTaskExternalCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.ExecuteTaskPipeCommand;
import es.bsc.compss.execution.invokers.binding.BindingInvoker;
import es.bsc.compss.execution.invokers.binding.ExternalTaskStatus;
import es.bsc.compss.execution.invokers.test.utils.FakeInvocation;
import es.bsc.compss.execution.invokers.test.utils.FakeInvocationContext;
import es.bsc.compss.execution.invokers.test.utils.FakeInvocationParam;
import es.bsc.compss.execution.platform.InvocationResources;
import es.bsc.compss.semantics.data.DataType;
import es.bsc.compss.semantics.task.parameter.StdIOStream;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import org.junit.Test;

public class TaskResultApplicationTest {

    @Test
    public void testZeroExitValueDoesNotFail() throws IOException, JobExecutionException {
        ExecutionSandbox sandBoxDir = createTempDirectory();
        TestBindingInvoker invoker = buildInvoker(11, null, null, null, sandBoxDir);

        invoker.runCheckExitValue(0);

        sandBoxDir.clean();
    }

    @Test
    public void testNonZeroExitValueFailsWithUnifiedMessage() throws IOException, JobExecutionException {
        ExecutionSandbox sandBoxDir = createTempDirectory();
        TestBindingInvoker invoker = buildInvoker(11, null, null, null, sandBoxDir);

        try {
            invoker.runCheckExitValue(1);
            fail("Non-zero exit value must fail the job");
        } catch (JobExecutionException e) {
            assertEquals("Job 11 has failed. Exit value is 1", e.getMessage());
        }

        sandBoxDir.clean();
    }

    @Test
    public void testCollectiveResultApplication() throws IOException, JobExecutionException {
        ExecutionSandbox sandBoxDir = createTempDirectory();
        FakeInvocationParam subParam0 = newExternalPscoParam("old0");
        FakeInvocationParam subParam1 = newExternalPscoParam("old1");
        FakeCollectionParam collectionParam =
            new FakeCollectionParam(new File(sandBoxDir.getFolder(), "collection.txt").getAbsolutePath());
        collectionParam.addElement(subParam0);
        collectionParam.addElement(subParam1);
        LinkedList<InvocationParam> params = new LinkedList<>();
        params.add(collectionParam);

        // Regression test: the former persistent updateParam cast the parameter instead of the result,
        // raising a ClassCastException for any collective parameter
        TestBindingInvoker invoker = buildInvoker(5, params, null, null, sandBoxDir);
        ExternalTaskStatus taskStatus = new ExternalTaskStatus("END_TASK 5 0 1 27 [(13,new0),(13,new1)]".split(" "));

        invoker.runApplyTaskResults(taskStatus);

        assertEquals(DataType.EXTERNAL_PSCO_T, subParam0.getType());
        assertEquals("new0", subParam0.getValue());
        assertEquals(DataType.EXTERNAL_PSCO_T, subParam1.getType());
        assertEquals("new1", subParam1.getValue());
        sandBoxDir.clean();
    }

    @Test
    public void testNonCollectiveResultLeavesCollectiveParamUntouched() throws IOException, JobExecutionException {
        ExecutionSandbox sandBoxDir = createTempDirectory();
        FakeInvocationParam subParam0 = newExternalPscoParam("old0");
        FakeInvocationParam subParam1 = newExternalPscoParam("old1");
        FakeCollectionParam collectionParam =
            new FakeCollectionParam(new File(sandBoxDir.getFolder(), "collection.txt").getAbsolutePath());
        collectionParam.addElement(subParam0);
        collectionParam.addElement(subParam1);
        LinkedList<InvocationParam> params = new LinkedList<>();
        params.add(collectionParam);

        // Regression test: the python bindings report dictionary collections as a single null-valued result of
        // the parameter's own type, which must leave the parameter untouched instead of failing the job
        ExternalTaskStatus taskStatus = new ExternalTaskStatus("END_TASK 5 0 1 28 null".split(" "));

        buildInvoker(5, params, null, null, sandBoxDir).runApplyTaskResults(taskStatus);

        assertEquals("old0", subParam0.getValue());
        assertEquals("old1", subParam1.getValue());
        sandBoxDir.clean();
    }

    @Test
    public void testResultApplicationOrder() throws IOException, JobExecutionException {
        ExecutionSandbox sandBoxDir = createTempDirectory();
        FakeInvocationParam param = newExternalPscoParam("oldParam");
        FakeInvocationParam target = newExternalPscoParam("oldTarget");
        FakeInvocationParam result = newExternalPscoParam("oldResult");
        LinkedList<InvocationParam> params = new LinkedList<>();
        params.add(param);
        LinkedList<InvocationParam> results = new LinkedList<>();
        results.add(result);

        // The task results are applied to the parameters, the target and the results, in that order
        TestBindingInvoker invoker = buildInvoker(7, params, target, results, sandBoxDir);
        ExternalTaskStatus taskStatus =
            new ExternalTaskStatus("END_TASK 7 0 3 13 newParam 13 newTarget 13 newResult".split(" "));

        invoker.runApplyTaskResults(taskStatus);

        assertEquals("newParam", param.getValue());
        assertEquals("newTarget", target.getValue());
        assertEquals("newResult", result.getValue());
        sandBoxDir.clean();
    }

    @Test
    public void testMissingResultsLeaveParametersUntouched() throws IOException, JobExecutionException {
        ExecutionSandbox sandBoxDir = createTempDirectory();
        FakeInvocationParam param = newExternalPscoParam("oldParam");
        FakeInvocationParam target = newExternalPscoParam("oldTarget");
        FakeInvocationParam result = newExternalPscoParam("oldResult");
        LinkedList<InvocationParam> params = new LinkedList<>();
        params.add(param);
        LinkedList<InvocationParam> results = new LinkedList<>();
        results.add(result);

        TestBindingInvoker invoker = buildInvoker(7, params, target, results, sandBoxDir);
        ExternalTaskStatus taskStatus = new ExternalTaskStatus("END_TASK 7 0 0".split(" "));

        invoker.runApplyTaskResults(taskStatus);

        assertEquals("oldParam", param.getValue());
        assertEquals("oldTarget", target.getValue());
        assertEquals("oldResult", result.getValue());
        sandBoxDir.clean();
    }

    private static ExecutionSandbox createTempDirectory() throws IOException {
        Path tempPath = Files.createTempDirectory("temp");
        File temp = tempPath.toFile();
        temp.deleteOnExit();
        return new ExecutionSandbox(temp, true);
    }

    private AbstractMethodImplementation genDummy(String className, String methodName) {
        return new AbstractMethodImplementation(0, 0,
            new ImplementationDescription<>(new MethodDefinition(COMPSsConstants.Lang.UNKNOWN, className, methodName),
                "", false, new MethodResourceDescription(), null, null));
    }

    private TestBindingInvoker buildInvoker(int jobId, List<InvocationParam> params, InvocationParam target,
        List<InvocationParam> results, ExecutionSandbox sandbox) throws JobExecutionException {

        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setJobId(jobId);
        if (params != null) {
            invBr = invBr.setParams(params);
        }
        if (target != null) {
            invBr = invBr.setTarget(target);
        }
        if (results != null) {
            invBr = invBr.setResult(results);
        }
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod"));

        FakeInvocation invocation = invBr.build();
        FakeInvocationContext context = new FakeInvocationContext.Builder().build();
        InvocationResources assignedResources = new InvocationResources(new int[0], new int[0], new int[0]);
        return new TestBindingInvoker(context, invocation, sandbox, assignedResources);
    }

    private FakeInvocationParam newExternalPscoParam(String value) {
        FakeInvocationParam param = new FakeInvocationParam(DataType.EXTERNAL_PSCO_T, "", "none", "",
            StdIOStream.UNSPECIFIED, 1.0, false, "", "", false);
        param.setValue(value);
        return param;
    }


    private static class TestBindingInvoker extends BindingInvoker {

        TestBindingInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
            InvocationResources assignedResources) throws JobExecutionException {
            super(context, invocation, sandbox, assignedResources);
        }

        @Override
        protected ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context, Invocation invocation,
            String sandBox, InvocationResources assignedResources) {
            return new ExecuteTaskPipeCommand(invocation.getJobId(), sandBox);
        }

        @Override
        protected void invokeExternalMethod() throws JobExecutionException {
            // Tests drive the shared task-result handling directly
        }

        @Override
        protected void cancelMethod() {
            // Nothing to do
        }

        void runCheckExitValue(int exitValue) throws JobExecutionException {
            checkExitValue(exitValue);
        }

        void runApplyTaskResults(ExternalTaskStatus taskStatus) throws JobExecutionException {
            applyTaskResults(taskStatus);
        }
    }

    private static class FakeCollectionParam extends FakeInvocationParam
        implements InvocationParamCollection<FakeInvocationParam> {

        private final List<FakeInvocationParam> elements;


        FakeCollectionParam(String collectionFilePath) {
            super(DataType.COLLECTION_T, "", "none", "", StdIOStream.UNSPECIFIED, 1.0, false, "", "", false);
            setValue(collectionFilePath);
            this.elements = new LinkedList<>();
        }

        @Override
        public boolean isCollective() {
            return true;
        }

        @Override
        public List<FakeInvocationParam> getCollectionParameters() {
            return this.elements;
        }

        @Override
        public void addElement(FakeInvocationParam param) {
            this.elements.add(param);
        }

        @Override
        public int getSize() {
            return this.elements.size();
        }
    }

}
