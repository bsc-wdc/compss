/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package storage;

public class CallbackEvent {

    public static enum EventType {
        FAIL, 
        SUCCESS;

        private EventType() {
        }
    }


    private String requestID;
    private EventType type;
    private Object content;


    public CallbackEvent() {

    }

    public CallbackEvent(String executeTaskId, EventType eventStatus, Object returnObject) {
        setRequestID(executeTaskId);
        setType(eventStatus);
        setContent(returnObject);
    }

    public String getRequestID() {
        return this.requestID;
    }

    public void setRequestID(String paramString) {
        if (paramString == null) {
            throw new IllegalArgumentException("requestID cannot be null");
        }
        this.requestID = paramString;
    }

    public EventType getType() {
        return this.type;
    }

    public void setType(EventType paramEventType) {
        if (paramEventType == null) {
            throw new IllegalArgumentException("type cannot be null");
        }
        this.type = paramEventType;
    }

    public Object getContent() {
        return this.content;
    }

    public void setContent(Object paramObject) {
        this.content = paramObject;
    }

}
