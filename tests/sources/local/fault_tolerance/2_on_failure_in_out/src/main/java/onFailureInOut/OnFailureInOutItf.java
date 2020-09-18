package onFailureInOut;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.task.Method;


public interface OnFailureInOutItf {

    @Method(declaringClass = "onFailureInOut.OnFailureInOutImpl", onFailure = OnFailure.CANCEL_SUCCESSORS)
    void processParamRead(@Parameter(type = Type.FILE, direction = Direction.INOUT) String filename);

    @Method(declaringClass = "onFailureInOut.OnFailureInOutImpl", onFailure = OnFailure.CANCEL_SUCCESSORS)
    void processParamWrite(@Parameter(type = Type.FILE, direction = Direction.IN) String filename,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String filename2);

}
