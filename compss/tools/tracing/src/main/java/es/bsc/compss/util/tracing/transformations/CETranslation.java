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
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.types.tracing.paraver.PRVLine;

import es.bsc.compss.util.tracing.EventTranslator;
import es.bsc.compss.util.tracing.TraceTransformation;

import java.util.Map;
import java.util.Map.Entry;


public class CETranslation implements TraceTransformation {

    private static final String CE_CODE = Integer.toString(TraceEventType.TASKS_FUNC.code);
    private final EventTranslator<String> translation;
    private final Map<String, String> labels;


    public CETranslation(EventTranslator<String> translation, Map<String, String> labels) {
        this.translation = translation;
        this.labels = labels;
    }

    @Override
    public void apply(SystemComposition infrastructure, ApplicationComposition threadOrganization) {
        // Do nothing
    }

    @Override
    public void apply(EventsDefinition events) {
        events.redefineCEs(labels);
    }

    @Override
    public void apply(PRVLine prvLine) {
        prvLine.translateEventsFromGroup(CE_CODE, translation);
    }

    @Override
    public String getDescription() {
        StringBuilder sb = new StringBuilder("Core Element (Event " + CE_CODE + ") translation:\n");
        for (Entry<String, String> e : translation.getAllTranslations().entrySet()) {
            sb.append("\t * ").append(e.getKey()).append("-->").append(e.getValue()).append("\n");
        }
        return sb.toString();
    }

}
