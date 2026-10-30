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
package es.bsc.compss.execution.invokers;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.execution.executor.ExecutionSandbox;
import es.bsc.compss.execution.executor.Executor;
import es.bsc.compss.execution.executor.ExecutorContext;
import es.bsc.compss.execution.invokers.binary.BinaryInvoker;
import es.bsc.compss.execution.invokers.binary.ContainerInvoker;
import es.bsc.compss.execution.invokers.binary.DecafInvoker;
import es.bsc.compss.execution.invokers.binary.JuliaInvoker;
import es.bsc.compss.execution.invokers.binary.MPIInvoker;
import es.bsc.compss.execution.invokers.binary.MpmdMPIInvoker;
import es.bsc.compss.execution.invokers.binary.OmpSsInvoker;
import es.bsc.compss.execution.invokers.binary.RunCOMPSsInvoker;
import es.bsc.compss.execution.invokers.binding.PythonMPIInvoker;
import es.bsc.compss.execution.platform.InvocationResources;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.MethodType;

public class BinaryInvokerFactory implements InvokerFactory {

    @Override
    public MethodType[] getSupportedTypes() {
        return new MethodType[] { MethodType.CONTAINER,
            MethodType.BINARY,
            MethodType.PYTHON_MPI,
            MethodType.MPI,
            MethodType.MPMDMPI,
            MethodType.COMPSs,
            MethodType.DECAF,
            MethodType.JULIA,
            MethodType.OMPSS,
            MethodType.OPENCL };
    }

    @Override
    public void preLoad(ExecutorContext platform, MethodType mt, COMPSsConstants.Lang lang) {
        // Do nothing
    }

    @Override
    public Invoker<?> build(Executor executor, InvocationContext context, Invocation inv, ExecutionSandbox wDir,
        InvocationResources resources) throws JobExecutionException {
        Invoker<?> invoker = null;
        MethodType mt = inv.getMethodImplementation().getMethodType();
        switch (mt) {
            case CONTAINER:
                invoker = new ContainerInvoker(context, inv, wDir, resources);
                break;
            case BINARY:
                invoker = new BinaryInvoker(context, inv, wDir, resources);
                break;
            case PYTHON_MPI:
                invoker = new PythonMPIInvoker(context, inv, wDir, resources);
                break;
            case MPI:
                invoker = new MPIInvoker(context, inv, wDir, resources);
                break;
            case MPMDMPI:
                invoker = new MpmdMPIInvoker(context, inv, wDir, resources);
                break;
            case COMPSs:
                invoker = new RunCOMPSsInvoker(context, inv, wDir, resources);
                break;
            case DECAF:
                invoker = new DecafInvoker(context, inv, wDir, resources);
                break;
            case JULIA:
                invoker = new JuliaInvoker(context, inv, wDir, resources);
                break;
            case OMPSS:
                invoker = new OmpSsInvoker(context, inv, wDir, resources);
                break;
            default:
                throw new JobExecutionException("Unrecognised implementation type " + mt);
        }
        return invoker;
    }

}
