package onFailureRetry;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.task.Method;


public interface OnFailureRetryItf {

    @Method(declaringClass = "onFailureRetry.OnFailureRetryImpl", onFailure = OnFailure.RETRY)
    void processParamRetry(@Parameter(type = Type.FILE, direction = Direction.INOUT) String filename);

}
