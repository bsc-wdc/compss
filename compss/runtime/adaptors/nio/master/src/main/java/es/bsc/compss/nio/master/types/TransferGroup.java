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
package es.bsc.compss.nio.master.types;

import es.bsc.compss.nio.master.NIOJob;
import es.bsc.compss.types.data.operation.copy.Copy;
import java.util.LinkedList;
import java.util.List;


public class TransferGroup {

    private final int id;
    private NIOJob job;
    private final List<Copy> copies;


    /**
     * Constructs a no-job-related transfer group with a specific group id.
     *
     * @param id id of the TransferGroup.
     */
    public TransferGroup(int id) {
        this.id = id;
        this.copies = new LinkedList<>();
    }

    public int getId() {
        return this.id;
    }

    /**
     * Adds a copy into the TransferGroup.
     * 
     * @param c Copy to include in the transfer group.
     */
    public void addCopy(Copy c) {
        this.copies.add(c);
    }

    /**
     * Returns the list of copies for the group.
     * 
     * @return the list of copies for the group.
     */
    public List<Copy> getCopies() {
        return this.copies;
    }

    /**
     * Binds the transfergroup to a specific job and its listener.
     * 
     * @param job job to bind to the transfergroup
     */
    public void bindToJob(NIOJob job) {
        this.job = job;
    }

    /**
     * Returns the job bound to the group.
     * 
     * @return bound job
     */
    public NIOJob getJob() {
        return job;
    }

}
