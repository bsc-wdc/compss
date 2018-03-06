/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

/**
 * Implementation of CallBacks for Storage ITF calls
 * 
 */
public class CallbackEvent {

    /**
     * Callback event type
     *
     */
    public static enum EventType {
        FAIL, // Call failed
        SUCCESS; // Call success

        private EventType() {
        }
    }


    private String requestID;
    private EventType type;
    private Object content;


    /**
     * Instantiate a new empty callback
     */
    public CallbackEvent() {

    }

    /**
     * Instantiate a callback for task @executeTaskId, eventStatus @eventStatus and return object @returnObject
     * 
     * @param executeTaskId
     * @param eventStatus
     * @param returnObject
     */
    public CallbackEvent(String executeTaskId, EventType eventStatus, Object returnObject) {
        setRequestID(executeTaskId);
        setType(eventStatus);
        setContent(returnObject);
    }

    /**
     * Returns the task ID
     * 
     * @return
     */
    public String getRequestID() {
        return this.requestID;
    }

    /**
     * Sets the task id
     * 
     * @param paramString
     */
    public void setRequestID(String paramString) {
        if (paramString == null) {
            throw new IllegalArgumentException("requestID cannot be null");
        }
        this.requestID = paramString;
    }

    /**
     * Returns the request event type
     * 
     * @return
     */
    public EventType getType() {
        return this.type;
    }

    /**
     * Sets the request event type
     * 
     * @param paramEventType
     */
    public void setType(EventType paramEventType) {
        if (paramEventType == null) {
            throw new IllegalArgumentException("type cannot be null");
        }
        this.type = paramEventType;
    }

    /**
     * Returns the content
     * 
     * @return
     */
    public Object getContent() {
        return this.content;
    }

    /**
     * Sets the content
     * 
     * @param paramObject
     */
    public void setContent(Object paramObject) {
        this.content = paramObject;
    }

}
