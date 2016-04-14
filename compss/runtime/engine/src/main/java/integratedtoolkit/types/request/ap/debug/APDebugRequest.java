package integratedtoolkit.types.request.ap.debug;

import integratedtoolkit.types.request.ap.APRequest;


/**
 * The TPRequest class represents any interaction with the TaskProcessor
 * component.
 */
public abstract class APDebugRequest extends APRequest {

    /**
     * Contains the different types of request that the Task Processor can
     * response.
     */
    public enum DebugRequestType {

    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.DEBUG;
    }

    public abstract DebugRequestType getDebugRequestType();
}
