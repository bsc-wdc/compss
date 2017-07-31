package es.bsc.compss.nio.worker.executors.util;

import java.io.File;
import java.lang.reflect.Method;
import java.util.concurrent.Semaphore;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.exceptions.JobExecutionException;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.types.implementations.MethodImplementation;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;
import javassist.bytecode.Descriptor;

import storage.CallbackEvent;
import storage.CallbackHandler;
import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


public class JavaInvoker extends Invoker {

    private static final String ERROR_CLASS_REFLECTION = "Cannot get class by reflection";
    private static final String ERROR_METHOD_REFLECTION = "Cannot get method by reflection";
    private static final String ERROR_CLASS_NOT_FOUND = "ERROR: Target object class not found";
    private static final String ERROR_EXTERNAL_NO_PSCO = "ERROR: External ExecuteTask can only be used with target PSCOs";
    private static final String ERROR_STORAGE_CALL = "ERROR: External executeTask call failed";
    private static final String ERROR_CALLBACK_INTERRUPTED = "ERROR: External callback interrupted";
    private static final String ERROR_EXTERNAL_EXECUTION = "ERROR: External Task Execution failed";
    private static final String WARN_RET_VALUE_EXCEPTION = "WARN: Exception on externalExecution return value";

    private final String className;
    private final String methodName;
    private final Method method;


    public JavaInvoker(NIOWorker nw, NIOTask nt, File taskSandboxWorkingDir, int[] assignedCoreUnits) throws JobExecutionException {
        super(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);

        // Get method definition properties
        MethodImplementation methodImpl = null;
        try {
            methodImpl = (MethodImplementation) this.impl;
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.methodType, e);
        }
        this.className = methodImpl.getDeclaringClass();
        this.methodName = methodImpl.getAlternativeMethodName();

        // Use reflection to get the requested method
        this.method = getMethod();
    }

    @Override
    public Object invokeMethod() throws JobExecutionException {
        return invokeJavaMethod();
    }

    private Method getMethod() throws JobExecutionException {
        Class<?> methodClass = null;
        Method method = null;
        try {
            methodClass = Class.forName(this.className);
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_CLASS_REFLECTION, e);
        }
        try {
            method = methodClass.getMethod(this.methodName, this.types);
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_REFLECTION, e);
        }

        return method;
    }

    private Object invokeJavaMethod() throws JobExecutionException {
        /* Invoke the requested method ******************************* */
        Object retValue = null;

        if (NIOWorker.getExecutionType().equals(COMPSsConstants.EXECUTION_INTERNAL)) {
            // Invoke the requested method from COMPSs
            retValue = internalExecution();
        } else {
            retValue = externalExecution();
        }

        return retValue;
    }

    private Object internalExecution() throws JobExecutionException {
        Object retValue = null;

        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.Event.STORAGE_INVOKE.getId(), NIOTracer.Event.STORAGE_INVOKE.getType());
        }

        try {
            LOGGER.info("Invoked " + method.getName() + " of " + target + " in " + nw.getHostName());
            retValue = method.invoke(target.getValue(), values);
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_TASK_EXECUTION, e);
        } finally {
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.STORAGE_INVOKE.getType());
            }
        }

        return retValue;
    }

    private Object externalExecution() throws JobExecutionException {
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
            descriptor = method.getName() + Descriptor.ofMethod(pool.getCtClass(method.getReturnType().getName()), ctParams);
        } catch (NotFoundException e) {
            throw new JobExecutionException(ERROR_CLASS_NOT_FOUND + " " + method.getReturnType().getName(), e);
        }

        // Check and retrieve target PSCO Id
        String id = null;
        try {
            id = ((StubItf) target.getValue()).getID();
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_EXTERNAL_NO_PSCO, e);
        }
        if (id == null) {
            throw new JobExecutionException(ERROR_EXTERNAL_NO_PSCO);
        }

        // Call Storage executeTask
        if (LOGGER.isDebugEnabled()) {
            LOGGER.info("External ExecuteTask " + method.getName() + " with target PSCO Id " + id + " in " + nw.getHostName());
        } else {
            LOGGER.info("External ExecuteTask " + method.getName());
        }

        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.Event.STORAGE_EXECUTETASK.getId(), NIOTracer.Event.STORAGE_EXECUTETASK.getType());
        }

        PSCOCallbackHandler callback = new PSCOCallbackHandler();
        try {
            String call_result = StorageItf.executeTask(id, descriptor, values, nw.getHostName(), callback);

            LOGGER.debug(call_result);

            // Wait for execution
            callback.waitForCompletion();
        } catch (StorageException e) {
            throw new JobExecutionException(ERROR_STORAGE_CALL, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JobExecutionException(ERROR_CALLBACK_INTERRUPTED, e);
        } finally {
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.STORAGE_EXECUTETASK.getType());
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

        return retValue;
    }


    /**
     * Class to get the Storage Callback
     * 
     */
    private class PSCOCallbackHandler extends CallbackHandler {

        private CallbackEvent event;
        private Semaphore sem;


        public PSCOCallbackHandler() {
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
