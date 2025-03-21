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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public interface EventsDefinition {

    /**
     * Appends new HWCounters to the already defined ones.
     *
     * @param allHWCounters new hardware counters definitions
     */
    public void defineNewHWCounters(Collection<String> allHWCounters);

    /**
     * Finds the CEs in the trace and create a mapping with the correspondence CE ID --> signature.
     *
     * @return mapping Core Element ID to Signature
     * @throws IOException Could not read the file
     * @throws MalformedException Malformed line in PCF
     */
    public Map<String, String> getCEsMapping() throws IOException, MalformedException;

    /**
     * Re-defines the CE events in the trace replacing the old correspondence by new labels.
     * 
     * @param labels new mapping Core Element ID to Signature
     */
    public void redefineCEs(Map<String, String> labels);

    /**
     * Checks the HW Counters enabled on the trace.
     *
     * @return list HW Counters definitions enabled on the trace
     * @throws IOException Error reading the trace
     * @throws MalformedException Malformed line in PCF
     */
    public List<String> getHWCounters() throws IOException, MalformedException;

}
