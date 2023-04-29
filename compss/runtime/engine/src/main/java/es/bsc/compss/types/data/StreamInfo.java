/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.data;

import es.bsc.compss.types.data.DataParams.StreamData;
import java.util.concurrent.Semaphore;


public class StreamInfo extends DataInfo<StreamData> {

    /**
     * Creates a new StreamInfo instance for the given stream.
     *
     * @param stream description of the stream related to the info
     */
    public StreamInfo(StreamData stream) {
        super(stream);
    }

    /**
     * Returns the object hashcode.
     *
     * @return The object hashcode.
     */
    public int getCode() {
        return this.getParams().getCode();
    }

    @Override
    public void willBeWritten() {
        // Do not increase version on write, since Stream just publish values
        this.currentVersion.willBeWritten();
        this.currentVersion.versionUsed();
    }

    @Override
    public void waitForDataReadyToDelete(Semaphore sem) {
        // Nothing to wait for
    }

}
