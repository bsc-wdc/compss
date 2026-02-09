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
package es.bsc.compss.types.tracing;

import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;

import java.util.function.Supplier;


public class APITracer {

    /**
     * Wraps a method returning an object with the start and end events.
     *
     * @param event event to wrap the method
     * @param work method to execute
     * @param <T> type of return
     * @return return of the method to execute
     */
    public static <T> T traced(APIEvent event, Supplier<T> work) {
        boolean active = Tracer.isActivated();
        if (active) {
            Tracer.emitEvent(event);
        }
        try {
            return work.get();
        } finally {
            if (active) {
                Tracer.emitEventEnd(event);
            }
        }
    }

    /**
     * Wraps a void method with the start and end events.
     *
     * @param event event to wrap the method
     * @param work method to execute
     */
    public static void traced(APIEvent event, Runnable work) {
        boolean active = Tracer.isActivated();
        if (active) {
            Tracer.emitEvent(event);
        }
        try {
            work.run();
        } finally {
            if (active) {
                Tracer.emitEventEnd(event);
            }
        }
    }


    @FunctionalInterface
    public interface ThrowingRunnable {

        void run() throws COMPSsException;
    }


    /**
     * Wraps a void method with the start and end events.
     *
     * @param event event to wrap the method
     * @param work method to execute
     * @throws COMPSsException exception raised by the user during the method execution.
     */
    public static void traced(APIEvent event, ThrowingRunnable work) throws COMPSsException {
        boolean active = Tracer.isActivated();
        if (active) {
            Tracer.emitEvent(event);
        }
        try {
            work.run();
        } finally {
            if (active) {
                Tracer.emitEventEnd(event);
            }
        }
    }
}
