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
package es.bsc.compss.invokers;

import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.Tracer;

import java.util.List;
import java.util.concurrent.Semaphore;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;
import javassist.bytecode.Descriptor;

import storage.CallbackEvent;
import storage.CallbackHandler;
import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


public class StorageInvoker extends JavaInvoker {

    private static final String ERROR_CLASS_NOT_FOUND = "ERROR: Target object class not found";
    private static final String ERROR_EXTERNAL_NO_PSCO =
        "ERROR: External ExecuteTask can only be used with target PSCOs";
    private static final String ERROR_STORAGE_CALL = "ERROR: External executeTask call failed";
    private static final String ERROR_CALLBACK_INTERRUPTED = "ERROR: External callback interrupted";
    private static final String ERROR_EXTERNAL_EXECUTION = "ERROR: External Task Execution failed";
    private static final String WARN_RET_VALUE_EXCEPTION = "WARN: Exception on externalExecution return value";


    public StorageInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandBox,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, sandBox, assignedResources);
    }

    @Override
    public void runMethod() throws JobExecutionException {
        // Invoke the requested method from the external platform

        // WARN: ExternalExecution is only supported for methods with PSCO as target object
        int n = method.getParameterAnnotations().length;
        ClassPool pool = ClassPool.getDefault();
        Class<?>[] cParams = method.getParameterTypes();
        CtClass[] ctParams = new CtClass[n];
        for (int i = 0; i < n; i++) {
            try {
                ctParams[i] = pool.getCtClass(((Class<?>) cParams[i]).getName());
            } catch (NotFoundException e) {
                throw new JobExecutionException(ERROR_CLASS_NOT_FOUND + " " + cParams[i].getName(), e);
            }
        }

        String descriptor;
        try {
            descriptor =
                method.getName() + Descriptor.ofMethod(pool.getCtClass(method.getReturnType().getName()), ctParams);
        } catch (NotFoundException e) {
            throw new JobExecutionException(ERROR_CLASS_NOT_FOUND + " " + method.getReturnType().getName(), e);
        }

        // Check and retrieve target PSCO Id
        String id = null;
        try {
            id = ((StubItf) this.invocation.getTarget().getValue()).getID();
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_EXTERNAL_NO_PSCO, e);
        }
        if (id == null) {
            throw new JobExecutionException(ERROR_EXTERNAL_NO_PSCO);
        }

        // Call Storage executeTask
        if (LOGGER.isDebugEnabled()) {
            LOGGER.info("External ExecuteTask " + method.getName() + " with target PSCO Id " + id + " in "
                + context.getHostName());
        } else {
            LOGGER.info("External ExecuteTask " + method.getName());
        }

        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.STORAGE_EXECUTETASK);
        }

        List<? extends InvocationParam> params = invocation.getParams();
        Object[] values = new Object[params.size()];
        int paramIdx = 0;
        for (InvocationParam param : params) {
            values[paramIdx++] = param.getValue();
        }

        CallbackHandlerPSCO callback = new CallbackHandlerPSCO();
        try {
            String callResult = StorageItf.executeTask(id, descriptor, values, context.getHostName(), callback);

            LOGGER.debug(callResult);

            // Wait for execution
            callback.waitForCompletion();
        } catch (StorageException e) {
            throw new JobExecutionException(ERROR_STORAGE_CALL, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JobExecutionException(ERROR_CALLBACK_INTERRUPTED, e);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(TraceEvent.STORAGE_EXECUTETASK);
            }
        }

        // Process the return status
        CallbackEvent.EventType callStatus = callback.getStatus();
        if (!callStatus.equals(CallbackEvent.EventType.SUCCESS)) {
            throw new JobExecutionException(ERROR_EXTERNAL_EXECUTION);
        }

        // Process return value
        Object retValue = null;
        if (method.getReturnType().getName().compareTo(void.class.getName()) != 0) {
            try {
                retValue = callback.getResult();
            } catch (StorageException e) {
                LOGGER.warn(WARN_RET_VALUE_EXCEPTION, e);
                retValue = null;
            }
        }

        for (InvocationParam np : this.invocation.getResults()) {
            np.setValue(retValue);
            if (retValue != null) {
                np.setValueClass(retValue.getClass());
            } else {
                np.setValueClass(null);
            }
        }
    }


    /**
     * Class to get the Storage Callback.
     */
    private class CallbackHandlerPSCO extends CallbackHandler {

        private CallbackEvent event;
        private Semaphore sem;


        public CallbackHandlerPSCO() {
            this.sem = new Semaphore(0);
        }

        @Override
        protected void eventListener(CallbackEvent e) {
            this.event = e;
            LOGGER.debug("Received event task finished with callback id " + event.getRequestID());

            synchronized (this) {
                this.notifyAll();
            }
            this.sem.release();
        }

        public void waitForCompletion() throws InterruptedException {
            this.sem.acquire();
        }

        public CallbackEvent.EventType getStatus() {
            return this.event.getType();
        }

        public Object getResult() throws StorageException {
            return StorageItf.getResult(event);
        }

    }

}
