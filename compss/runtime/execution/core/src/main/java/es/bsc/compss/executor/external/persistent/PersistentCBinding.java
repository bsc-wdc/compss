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
package es.bsc.compss.executor.external.persistent;

/**
 * Bridge to the persistent binding implemented in C: loads the native library providing the binding and exposes its JNI
 * entry points, which are the transport that executes a task inside the binding of the calling thread and the
 * operations that bind and unbind threads to it.
 */
public final class PersistentCBinding {

    static {
        System.loadLibrary("bindings_common");
    }


    /**
     * Non-instantiable: the binding is only accessed through its static native entry points.
     */
    private PersistentCBinding() {
    }

    /**
     * Executes a task command inside the persistent binding of the calling thread.
     *
     * @param args Command describing the task to execute
     * @return Result reported by the binding for the executed task
     */
    public static native String executeInBinding(String args);

    /**
     * Initializes the persistent binding of the calling thread.
     */
    public static native void initThread();

    /**
     * Finishes the persistent binding of the calling thread.
     */
    public static native void finishThread();

}
