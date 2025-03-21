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

package es.bsc.compss.types.request.listener;

import java.util.concurrent.Semaphore;


/**
 * The StaticMultioperationSemaphore implements a RequestListener that releases a semaphore as soons as a fixed number
 * of requests have been performed. If more the listener receives more notifications of requests performances then the
 * specified number, it will ignore the notifications. Warning: Not thread safe!
 */
public class StaticMultioperationSemaphore implements RequestListener {

    private final Semaphore sem;
    private int missingRequests;


    /**
     * Constructs a new StaticMultioperationSemaphore.
     *
     * @param numOperations operations pending to complete before releasing the semaphore
     * @param sem semaphore to release at the end of the operations
     */
    public StaticMultioperationSemaphore(int numOperations, Semaphore sem) {
        this.sem = sem;
        this.missingRequests = numOperations;
        if (numOperations <= 0) {
            sem.release();
        }
    }

    @Override
    public void performed() {
        missingRequests--;
        if (missingRequests == 0) {
            sem.release();
        }
    }

}
