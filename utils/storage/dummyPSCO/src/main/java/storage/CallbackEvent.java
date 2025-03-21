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
package storage;

public class CallbackEvent {

    public static enum EventType {
        FAIL, // Failed event
        SUCCESS; // Successful event
    }


    private String requestID;
    private EventType type;
    private Object content;


    /**
     * Builds a new CallbackEvent instance.
     */
    public CallbackEvent() {
        // Empty callback event
    }

    /**
     * Creates a new CallbackEvent instance with the given information.
     * 
     * @param executeTaskId Associated task.
     * @param eventStatus Event type.
     * @param returnObject Return object.
     */
    public CallbackEvent(String executeTaskId, EventType eventStatus, Object returnObject) {
        setRequestID(executeTaskId);
        setType(eventStatus);
        setContent(returnObject);
    }

    /**
     * Returns the request id.
     * 
     * @return The request id.
     */
    public String getRequestID() {
        return this.requestID;
    }

    /**
     * Returns the event type.
     * 
     * @return The event type.
     */
    public EventType getType() {
        return this.type;
    }

    /**
     * Returns the callback return content.
     * 
     * @return The callback return content.
     */
    public Object getContent() {
        return this.content;
    }

    /**
     * Sets a new request id.
     * 
     * @param paramString New request id.
     */
    public void setRequestID(String paramString) {
        if (paramString == null) {
            throw new IllegalArgumentException("requestID cannot be null");
        }
        this.requestID = paramString;
    }

    /**
     * Sets a new event type.
     * 
     * @param paramEventType Event type.
     */
    public void setType(EventType paramEventType) {
        if (paramEventType == null) {
            throw new IllegalArgumentException("type cannot be null");
        }
        this.type = paramEventType;
    }

    /**
     * Sets a new callback return content.
     * 
     * @param paramObject New callback return content.
     */
    public void setContent(Object paramObject) {
        this.content = paramObject;
    }

}
