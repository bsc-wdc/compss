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
package es.bsc.compss.api;

public class COMPSsGroup implements AutoCloseable {

    public String groupName;
    public Boolean barrier;


    public COMPSsGroup(String groupName, boolean implicitBarrier) {
        this.groupName = groupName;
        this.barrier = implicitBarrier;
    }

    public COMPSsGroup(String groupName) {
        this.groupName = groupName;
        this.barrier = true;
    }

    @Override
    public void close() throws Exception {
        System.out.println("Group " + groupName + " closed");
    }
}
