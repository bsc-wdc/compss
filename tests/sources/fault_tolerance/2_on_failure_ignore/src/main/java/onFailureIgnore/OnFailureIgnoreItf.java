package onFailureIgnore;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.task.Method;


public interface OnFailureIgnoreItf {

    @Method(declaringClass = "onFailureIgnore.OnFailureIgnoreImpl", onFailure = OnFailure.IGNORE)
    void processParamIgnoreFailure(@Parameter(type = Type.FILE, direction = Direction.INOUT) String filename);

    @Method(declaringClass = "onFailureIgnore.OnFailureIgnoreImpl", onFailure = OnFailure.IGNORE)
    void processOutParamIgnoreFailure(@Parameter(type = Type.FILE, direction = Direction.OUT) String filename1,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String filename2);
}
