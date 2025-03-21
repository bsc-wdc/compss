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
package es.bsc.compss.types.tracing;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public interface Trace {

    /**
     * Returns the directory where the trace is stored.
     *
     * @return name of the trace
     */
    public String getDirectory();

    /**
     * Returns the name of the trace.
     *
     * @return name of the trace
     */
    public String getName();

    public String getDate();

    /**
     * Returns the length of the trace and the time unit.
     * 
     * @return length of the trace and the time unit.
     */
    public String getDuration();

    /**
     * Returns the length of the trace in nanoseconds.
     * 
     * @return length of the trace in nanoseconds.
     */
    public long getDurationInNS();

    public SystemComposition getInfrastructure();

    public ApplicationComposition getThreadOrganization();

    public EventsDefinition getEventsDefinition();

    /**
     * Processes the trace to obtain the synchronization events.
     *
     * @param workerID identifier of the resource to which the trace belongs
     * @return map indicating the synchronization events found for each resource
     * @throws IOException error reading from the trace file
     */
    public Map<Integer, List<SynchEvent>> getSyncEvents(Integer workerID) throws IOException;


    public static interface RecordScanner extends Closeable {

        public String next() throws IOException;

        @Override
        public void close() throws IOException;
    }


    public RecordScanner getRecords() throws FileNotFoundException;


    public static interface RecordAppender extends Closeable {

        public void append(String event) throws IOException;

        @Override
        public void close() throws IOException;
    }


    public RecordAppender getRecordAppender() throws IOException;
}
