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
package es.bsc.compss.invokers.test.utils.types;

public class Event {

    public static enum Type {
        GET_OBJECT, GET_PERSISTENT_OBJECT, STORE_OBJECT, STORE_PERSISTENT_OBJECT, RUNNING_METHOD, METHOD_RETURN
    }


    private final Type type;
    private final Object expectedState;
    private final Object attachedReply;


    /**
     * Event constructor.
     * 
     * @param type Type
     * @param state State
     * @param reply Reply
     */
    public Event(Type type, Object state, Object reply) {
        this.type = type;
        this.expectedState = state;
        this.attachedReply = reply;
    }

    public Type getType() {
        return type;
    }

    public Object getExpectedState() {
        return expectedState;
    }

    public Object getAttachedReply() {
        return attachedReply;
    }

    @Override
    public String toString() {
        return type + "" + expectedState + " -> " + attachedReply;
    }
}
