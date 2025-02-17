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
package es.bsc.compss.util.tracing;

import java.util.HashMap;
import java.util.Map;


public class EventTranslator<T> {

    private final Map<T, T> finalMap;


    /**
     * Constructs a new EventTranslator.
     * 
     * @param localCE map input -> intermediate
     * @param globalCE map intermediate ->final
     */
    public EventTranslator(Map<T, T> localCE, Map<T, T> globalCE) {
        this.finalMap = new HashMap<>();
        for (T t : localCE.keySet()) {
            T eventIdentifier = localCE.get(t);
            T result = globalCE.get(eventIdentifier);
            this.finalMap.put(t, result);
        }
    }

    public T translateEvent(T value) {
        return finalMap.get(value);
    }

    public Map<T, T> getAllTranslations() {
        return this.finalMap;
    }

}
