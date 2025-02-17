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
package es.bsc.compss.util.tracing.transformations;

import es.bsc.compss.types.tracing.ApplicationComposition;
import es.bsc.compss.types.tracing.EventsDefinition;
import es.bsc.compss.types.tracing.SystemComposition;
import es.bsc.compss.types.tracing.paraver.PRVLine;
import es.bsc.compss.util.tracing.TraceTransformation;


public class CPUOffset implements TraceTransformation {

    private final int offset;


    public CPUOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public void apply(SystemComposition infrastructure, ApplicationComposition threadOrganization) {
        // Do nothing
    }

    @Override
    public void apply(EventsDefinition events) {
        // Do nothing
    }

    @Override
    public void apply(PRVLine prvLine) {
        prvLine.applyCPUOffset(offset);
    }

    @Override
    public String getDescription() {
        return "Applying a CPU offset of " + this.offset;
    }
}
