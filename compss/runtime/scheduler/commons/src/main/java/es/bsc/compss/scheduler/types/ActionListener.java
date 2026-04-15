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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.worker.COMPSsException;

/**
 * Listener interface for receiving lifecycle notifications about the execution of an {@link AllocatableAction}.
 * Implementations of this interface are notified by the runtime system when an action transitions through its execution
 * states. Listeners are passive observers and must not attempt to control or alter the execution of the action.
 * Listeners are bound to individual {@code AllocatableAction} instances and may be invoked synchronously by the
 * scheduler thread or others. * Implementations should therefore return quickly and must be thread-safe. Any exception
 * thrown by a listener implementation will be caught and ignored by the runtime system to prevent interference with
 * action execution.
 */
public interface ActionListener<A extends AllocatableAction> {

    /**
     * Invoked when the action enters the running state and execution is about to begin.
     *
     * @param action the action that has started execution
     */
    void onActionStarted(A action);

    /**
     * Invoked when the action has completed successfully.
     *
     * @param action the action that completed execution
     */
    void onActionCompleted(A action);

    /**
     * Invoked when an action fails.
     *
     * @param action Failed action.
     */
    void onActionFailed(A action);

    /**
     * Invoked when an unexpected COMPSs exception is raised during the execution of the action.
     *
     * @param action Action which raised the exception.
     * @param e COMPSs action exception.
     */
    void onActionException(A action, COMPSsException e);

}
