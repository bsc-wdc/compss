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
package es.bsc.compss.invokers;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.execution.PythonMirror;
import es.bsc.compss.execution.types.ExecutorContext;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.external.piped.PipedExecutor;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.definition.NativeDefinition;

public class PythonInvokerFactory implements InvokerFactory {

    @Override
    public MethodType[] getSupportedTypes() {
        return new MethodType[] { MethodType.METHOD,
            MethodType.MULTI_NODE, };
    }

    @Override
    public void preLoad(ExecutorContext platform, MethodType mt, COMPSsConstants.Lang lang) {
        if (mt == MethodType.METHOD || mt == MethodType.MULTI_NODE) {
            if (lang == COMPSsConstants.Lang.PYTHON) {
                PythonMirror pyMirror = new PythonMirror(platform);
                platform.registerMirror(PythonMirror.class, pyMirror);
            }
        }
    }

    @Override
    public Invoker<?> build(Executor executor, InvocationContext context, Invocation inv, ExecutionSandbox wDir,
        InvocationResources resources) throws JobExecutionException {
        COMPSsConstants.Lang lang = ((NativeDefinition) inv.getMethodImplementation().getDefinition()).getLang();
        if (lang == COMPSsConstants.Lang.PYTHON) {
            PipedExecutor pyExec = PythonMirror.getPipedExecutor(executor);
            return new PythonInvoker(context, inv, wDir, resources, pyExec);
        }
        return null;
    }
}
