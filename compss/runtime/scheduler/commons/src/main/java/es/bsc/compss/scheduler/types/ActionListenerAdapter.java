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
 * Convenience base implementation of {@link ActionListener}. All methods are empty, so subclasses can override only the
 * events they care about.
 */
public abstract class ActionListenerAdapter<A extends AllocatableAction> implements ActionListener<A> {

    @Override
    public void onActionStarted(A action) {
        // Do nothing
    }

    @Override
    public void onActionCompleted(A action) {
        // Do nothing
    }

    @Override
    public void onActionFailed(A action) {
        // Do nothing
    }

    @Override
    public void onActionException(A action, COMPSsException e) {
        // Do nothing
    }

}
