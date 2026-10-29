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

package es.bsc.compss.invokers.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.external.commands.ExecuteTaskExternalCommand;
import es.bsc.compss.executor.external.piped.commands.ExecuteTaskPipeCommand;
import es.bsc.compss.invokers.external.BindingInvoker;
import es.bsc.compss.invokers.test.utils.FakeInvocation;
import es.bsc.compss.invokers.test.utils.FakeInvocationContext;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.junit.Test;

public class HostlistAndAffinityTest {

    @Test
    public void testWorkersStringRepeatsEachHostnamePerComputingUnit() {
        List<String> hostnames = Arrays.asList("host1", "host2");

        assertEquals("host1,host1,host2,host2", TestBindingInvoker.runBuildWorkersString(hostnames, 2));
    }

    @Test
    public void testWorkersStringRemovesInfinibandSuffix() {
        List<String> hostnames = Arrays.asList("n1-ib0", "n2-ib0");

        assertEquals("n1,n2", TestBindingInvoker.runBuildWorkersString(hostnames, 1));
    }

    @Test
    public void testWorkersStringKeepsOnlyFirstHostnameWithZeroComputingUnits() {
        // Preserved original semantics: with no process to launch, only the first hostname is described
        List<String> hostnames = Arrays.asList("host1", "host2");

        assertEquals("host1", TestBindingInvoker.runBuildWorkersString(hostnames, 0));
    }

    @Test
    public void testWorkersStringEmptyHostlist() {
        assertEquals("", TestBindingInvoker.runBuildWorkersString(new LinkedList<>(), 4));
    }

    @Test
    public void testThreadAffinityJoinsAssignedCPUs() throws IOException, JobExecutionException {
        TestBindingInvoker invoker = buildInvoker(new int[] { 0,
            1,
            2 }, new int[0]);

        assertEquals("0,1,2", invoker.runAddThreadAffinity());
    }

    @Test
    public void testThreadAffinityNoAssignedCPUs() throws IOException, JobExecutionException {
        TestBindingInvoker invoker = buildInvoker(new int[0], new int[0]);

        assertEquals("-", invoker.runAddThreadAffinity());
    }

    @Test
    public void testGPUAffinityJoinsAssignedGPUs() throws IOException, JobExecutionException {
        TestBindingInvoker invoker = buildInvoker(new int[0], new int[] { 2,
            5 });

        assertEquals("2,5", invoker.runAddGPUAffinity());
    }

    @Test
    public void testGPUAffinityNoAssignedGPUs() throws IOException, JobExecutionException {
        TestBindingInvoker invoker = buildInvoker(new int[0], new int[0]);

        assertEquals("-", invoker.runAddGPUAffinity());
    }

    @Test
    public void testExternalCommandCarriesConsolidatedResourceArguments() throws IOException, JobExecutionException {
        // The external command ends with the thread affinity, the GPU affinity (no GPU assigned) and the hostlist
        // built from the slave nodes plus the master host, with the InfiniBand suffix removed and each hostname
        // repeated once per computing unit
        TestBindingInvoker invoker = buildInvoker(new int[] { 0,
            1,
            2 }, new int[0]);

        String command = invoker.getCommandAsString();
        assertTrue("Unexpected resource arguments in command: " + command,
            command.endsWith(" 0,1,2 - slave1,slave1,slave2,slave2,localhost,localhost"));
    }

    @Test
    public void testExternalCommandCarriesOverriddenAffinityArguments() throws IOException, JobExecutionException {
        // BindingInvoker inheritors can extend how CPU/GPU affinity is handled: the overridden hooks replace the
        // default affinity arguments in the built command, keeping the hostlist untouched
        CustomAffinityBindingInvoker invoker = new CustomAffinityBindingInvoker(buildContext(), buildInvocation(),
            buildSandbox(), new InvocationResources(new int[] { 0,
                1,
                2 }, new int[0], new int[0]));

        String command = invoker.getCommandAsString();
        assertTrue("Unexpected resource arguments in command: " + command,
            command.endsWith(" taskset -c 0-2 gpus:off slave1,slave1,slave2,slave2,localhost,localhost"));
    }

    private static ExecutionSandbox buildSandbox() throws IOException {
        Path tempPath = Files.createTempDirectory("temp");
        File temp = tempPath.toFile();
        temp.deleteOnExit();
        return new ExecutionSandbox(temp, true);
    }

    private static FakeInvocationContext buildContext() {
        return new FakeInvocationContext.Builder().build();
    }

    private static FakeInvocation buildInvocation() {
        // Fixed setup: two slave nodes (one with InfiniBand suffix) and a 2-CPU method task
        MethodResourceDescription requirements = new MethodResourceDescription();
        requirements.addProcessor("cpu", 2, "x86", 1.0f, "CPU", 0.0f, "", "");
        LinkedList<String> slaves = new LinkedList<>();
        slaves.add("slave1-ib0");
        slaves.add("slave2");
        FakeInvocation.Builder invBr = new FakeInvocation.Builder();
        invBr = invBr.setJobId(3);
        invBr = invBr.setImpl(genDummy("FakeClass", "fakeMethod"));
        invBr = invBr.setRequirements(requirements);
        invBr = invBr.setSlaves(slaves);
        return invBr.build();
    }

    private static TestBindingInvoker buildInvoker(int[] assignedCPUs, int[] assignedGPUs)
        throws IOException, JobExecutionException {
        return new TestBindingInvoker(buildContext(), buildInvocation(), buildSandbox(),
            new InvocationResources(assignedCPUs, assignedGPUs, new int[0]));
    }

    private static AbstractMethodImplementation genDummy(String className, String methodName) {
        return new AbstractMethodImplementation(0, 0,
            new ImplementationDescription<>(new MethodDefinition(COMPSsConstants.Lang.UNKNOWN, className, methodName),
                "", false, new MethodResourceDescription(), null, null));
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
            // Tests drive the command construction directly
        }

        @Override
        protected void cancelMethod() {
            // Nothing to do
        }

        String getCommandAsString() {
            return this.command.getAsString();
        }

        static String runBuildWorkersString(List<String> hostnames, int computingUnits) {
            return buildWorkersString(hostnames, computingUnits);
        }

        String runAddThreadAffinity() {
            return String.join(",", addThreadAffinity(this.assignedResources));
        }

        String runAddGPUAffinity() {
            return String.join(",", addGPUAffinity(this.assignedResources));
        }
    }

    private static class CustomAffinityBindingInvoker extends TestBindingInvoker {

        CustomAffinityBindingInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
            InvocationResources assignedResources) throws JobExecutionException {
            super(context, invocation, sandbox, assignedResources);
        }

        @Override
        protected ArrayList<String> addThreadAffinity(InvocationResources assignedResources) {
            // Custom CPU affinity handling: a wrapper command instead of the plain CPU list
            ArrayList<String> args = new ArrayList<>();
            args.add("taskset");
            args.add("-c");
            args.add("0-2");
            return args;
        }

        @Override
        protected ArrayList<String> addGPUAffinity(InvocationResources assignedResources) {
            // Custom GPU affinity handling: an explicit off switch instead of the no-affinity placeholder
            ArrayList<String> args = new ArrayList<>();
            args.add("gpus:off");
            return args;
        }
    }

}
