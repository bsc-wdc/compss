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
        FAIL, 
        SUCCESS;

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
