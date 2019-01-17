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
