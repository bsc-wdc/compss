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
package es.bsc.compss.util;

/**
 * The RequestCollection class is a utility gathering requests from a certain type. Any component can add a Request to
 * the collection. At any point of the execution a thread can poll a Request from the queue to treat it, if there are no
 * requests on the collection it falls asleep until a new request is enqueued.
 *
 * @param <R> Type of the Requests
 */
public interface RequestCollection<R> {

    /**
     * Adds a request at the tail of the queue.
     *
     * @param request Request to be added
     */
    void add(R request);

    /**
     * Polls a request from the collection, if available; otherwise it waits until one is added.
     *
     * @return polled request
     */
    R poll();

    /**
     * Removes a request from the collection.
     *
     * @param request Request to be removed from the collection.
     */
    void remove(R request);

    /**
     * Returns the number of requests.
     *
     * @return Number of requests in the collection.
     */
    int getSize();

}
