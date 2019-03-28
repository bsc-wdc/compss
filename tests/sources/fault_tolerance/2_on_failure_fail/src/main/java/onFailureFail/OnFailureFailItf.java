package onFailureFail;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.task.Method;


public interface OnFailureFailItf {

    @Method(declaringClass = "onFailureFail.OnFailureFailImpl", onFailure = OnFailure.FAIL)
    void processParamDirectFail(@Parameter(type = Type.FILE, direction = Direction.INOUT) String filename);
}
