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

import es.bsc.compss.types.tracing.ApplicationComposition;
import es.bsc.compss.types.tracing.ThreadIdentifier;


public interface ThreadTranslator<T extends ThreadIdentifier> {

    /**
     * Returns the new thread ID for an old one.
     *
     * @param oldThreadId old thread ID
     * @return new Thread ID
     */
    public T getNewThreadId(T oldThreadId);

    /**
     * Returns a list of the number of runtime threads per each machine.
     *
     * @return array indicating the number of runtime threads per each machine
     */
    public ApplicationComposition getNewThreadOrganization();

    /**
     * Returns a string describing how threads are translated.
     * 
     * @return thread translation description
     */
    public String getDescription();
}
