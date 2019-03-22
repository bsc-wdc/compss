package onFailureCancelSuccessors;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.task.Method;


public interface OnFailureCancelSuccessorsItf {

    @Method(declaringClass = "onFailureCancelSuccessors.OnFailureCancelSuccessorsImpl", onFailure = OnFailure.CANCEL_SUCCESSORS)
    void processParamCancelSuccessors(@Parameter(type = Type.FILE, direction = Direction.INOUT) String filename);

}
