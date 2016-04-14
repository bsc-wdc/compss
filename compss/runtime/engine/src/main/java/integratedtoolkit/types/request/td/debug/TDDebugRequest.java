package integratedtoolkit.types.request.td.debug;

import integratedtoolkit.types.request.td.TDRequest;


/**
 * The TPRequest class represents any interaction with the TaskProcessor
 * component.
 */
public abstract class TDDebugRequest extends TDRequest {

    /**
     * Contains the different types of request that the Task Processor can
     * response.
     */
    public enum DebugRequestType {

    }

    public TDRequestType getRequestType() {
        return TDRequestType.DEBUG;
    }

    public abstract DebugRequestType getDebugRequestType();
}
