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
package es.bsc.wdc.tracing;

import java.util.List;


public interface EventType {

    /**
     * Retuns the code identifying the Event type.
     * 
     * @return code identifying the Event type.
     */
    public int getCode();

    /**
     * Returns a description of the EventType.
     * 
     * @return description of the EventType
     */
    public String getDescription();

    /**
     * Returns whether the events of the type are endable or not.
     *
     * @return {@literal true} if they are endable; {@literal false} otherwise
     */
    public boolean isEndable();

    /**
     * Returns a list of possible events within the group.
     *
     * @return list of possible events within the group.
     */
    public List<Event> getEvents();
}
