package es.bsc.compss.executor.utils;

import es.bsc.compss.invokers.external.persistent.PersistentInvoker;
import es.bsc.compss.types.execution.InvocationContext;

public class PersistentMirror implements ExecutionPlatformMirror {

    public PersistentMirror(InvocationContext context, int size) {

    }

    @Override
    public void stop() {

    }

    @Override
    public void unregisterExecutor(String id) {
        PersistentInvoker.finishThread();
    }

    public void registerExecutor(String id) {
        PersistentInvoker.initThread();
    }
}
