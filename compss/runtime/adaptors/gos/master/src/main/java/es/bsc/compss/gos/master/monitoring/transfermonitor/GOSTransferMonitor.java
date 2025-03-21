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
package es.bsc.compss.gos.master.monitoring.transfermonitor;

public interface GOSTransferMonitor {

    /**
     * Checks status of the transfer and controls multiple steps in case of complex transfers (Different hosts). If it
     * detects the end of the transfers, notify the state and releases the allocated channels.
     * 
     * @return true if the transfer has ended
     */
    boolean monitor();

    /**
     * Method to easily identify the transfer monitor.
     * 
     * @return a String with identification information.
     */
    String toString();

    /**
     * Returns the id of the transfer, it must be unique.
     * 
     * @return the id of the transfer
     */
    int getID();

    void releaseResources();

    /**
     * Method in case to release all given resources and mark failure to the copy. It does not notify the copy to the
     * state.
     */
    void shutdown();

    /**
     * Gives the type of transfer.
     * 
     * @return Type of transfer.
     */
    String getType();

}
