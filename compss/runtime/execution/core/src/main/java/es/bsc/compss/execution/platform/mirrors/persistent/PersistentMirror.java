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
package es.bsc.compss.execution.platform.mirrors.persistent;

import es.bsc.compss.execution.InvocationContext;
import es.bsc.compss.execution.executor.Executor;
import es.bsc.compss.execution.executor.ExecutorContext;
import es.bsc.compss.execution.executor.binding.persistent.PersistentCBinding;
import es.bsc.compss.execution.executor.binding.persistent.PersistentExecutor;
import es.bsc.compss.execution.platform.mirrors.ExecutorContextMirror;
import es.bsc.compss.execution.platform.mirrors.MirrorFactory;
import es.bsc.compss.log.Loggers;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Mirror of the persistent executor context: registers and unregisters the persistent binding of executor threads and
 * hands out the handles that represent those bindings. Stopping the mirror invalidates the handles of threads that
 * finished without unregistering; job cancellation is not supported by persistent bindings.
 */
public class PersistentMirror implements ExecutorContextMirror<PersistentExecutor> {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);

    // Factory creating this mirror on first lazy registration
    private static final MirrorFactory<PersistentMirror> MIRROR_FACTORY = new MirrorFactory<PersistentMirror>() {

        @Override
        public PersistentMirror create(ExecutorContext platform) {
            return new PersistentMirror(platform);
        }
    };

    private final InvocationContext context;

    // Authoritative registry of the live thread bindings: it owns the executor lifecycles, and it is the structure
    // that unregisterExecutor looks up by executor and that stop() enumerates to invalidate survivors. All access is
    // synchronized on it
    private final Map<Executor, PersistentExecutor> registeredExecutors;

    // Cache of the current thread's executor, so the hot path does not re-run the thread registration (new handle,
    // JNI thread binding, registration log) per task. It is advisory: entries are validated through the canRun check
    // and re-registered on the mirror when stale, and stop() only removes the stopping thread's entry (other threads'
    // entries become inert garbage reclaimed lazily)
    private final ThreadLocal<PersistentExecutor> threadExecutor;


    /**
     * Creates a new PersistentMirror instance with the given context.
     *
     * @param platform Mirrored platform
     */
    public PersistentMirror(ExecutorContext platform) {
        this.context = platform.getContext();
        this.registeredExecutors = new HashMap<>();
        this.threadExecutor = new ThreadLocal<>();
    }

    /**
     * Returns the PersistentExecutor associated to an executor.
     *
     * @param e executor whose PersistentExecutor we want to obtain
     * @return PersistentExecutor corresponding to executor e
     */
    public static PersistentExecutor getPersistentExecutor(Executor e) {
        return e.getPlatform().getMirroredExecutor(e, PersistentMirror.class, MIRROR_FACTORY);
    }

    @Override
    public void stop() {
        // Executor threads unregister themselves before this method runs, finishing their own thread binding on
        // their own thread, so the registry is normally empty here. Handles still present belong to threads that
        // finished abnormally: they are only invalidated and logged, because the native finish operates on the
        // calling thread and would tear down the wrong binding. Only the stopping thread's own entry can be removed
        // from the thread handle cache; entries of other threads become stale and are re-created on their next lazy
        // lookup.
        Collection<PersistentExecutor> remaining;
        synchronized (this.registeredExecutors) {
            remaining = new ArrayList<>(this.registeredExecutors.values());
            this.registeredExecutors.clear();
        }
        for (PersistentExecutor handle : remaining) {
            LOGGER.warn("Executor {} finished abnormally: its persistent binding cannot be finished from the stopping"
                + " thread and is only invalidated", handle.getOwningExecutor());
            handle.terminated();
        }
        this.threadExecutor.remove();
    }

    @Override
    public void cancelJob(PersistentExecutor handle) {
        // Not supported by persistent bindings: the job runs as a native call on the executor thread and cannot
        // be interrupted from outside, so the request is only logged
        LOGGER.warn("Requested cancellation of a job running on persistent executor {}: not supported by persistent"
            + " bindings, nothing cancelled", handle.getOwningExecutor());
    }

    @Override
    public void unregisterExecutor(Executor executor) {
        // Invalidates the executor's handle and finishes its thread binding; unregistering a never-registered
        // executor is a no-op. The handle is atomically removed from the registry before the binding is finished,
        // so concurrent unregister requests of the same executor finish its binding exactly once
        PersistentExecutor pExecutor;
        synchronized (this.registeredExecutors) {
            pExecutor = this.registeredExecutors.remove(executor);
        }
        if (pExecutor != null) {
            PrintStream out = context.getThreadOutStream();
            // WARNING: Do not remove this log, is used for runtime testing
            out.println("[PersistentMirror] Thread unregistration has been done.");
            pExecutor.terminated();
            finishThreadBinding();
        }
    }

    @Override
    public PersistentExecutor registerExecutor(Executor executor) {
        // Creates and stores the handle, then initializes the thread binding; the handle only accepts tasks once
        // the initialization has completed (deployed below)
        PersistentExecutor pExecutor = new PersistentExecutor(this, executor);
        synchronized (this.registeredExecutors) {
            this.registeredExecutors.put(executor, pExecutor);
        }

        PrintStream out = context.getThreadOutStream();
        // WARNING: Do not remove this log, is used for runtime testing
        out.println("[PersistentMirror] Thread registration has been done.");
        initThreadBinding();
        pExecutor.deployed();
        return pExecutor;
    }

    @Override
    public PersistentExecutor getMirroredExecutor(Executor executor) {
        PersistentExecutor pExecutor = this.threadExecutor.get();
        if (pExecutor == null || !pExecutor.canRun()) {
            pExecutor = registerExecutor(executor);
            this.threadExecutor.set(pExecutor);
        }
        return pExecutor;
    }

    /**
     * Initializes the persistent binding of the current thread. Package-private and overridable so unit tests can
     * replace the native binding initialization with a no-op.
     */
    void initThreadBinding() {
        PersistentCBinding.initThread();
    }

    /**
     * Finishes the persistent binding of the current thread. Package-private and overridable so unit tests can replace
     * the native binding teardown with a no-op.
     */
    void finishThreadBinding() {
        PersistentCBinding.finishThread();
    }
}
