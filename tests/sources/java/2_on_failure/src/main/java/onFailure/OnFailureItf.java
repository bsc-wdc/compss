package onFailure;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.task.Method;


public interface OnFailureItf {
     
    @Method(declaringClass = "onFailure.OnFailureImpl", onFailure=OnFailure.IGNORE)
    void processParam(
            @Parameter(type = Type.FILE, direction = Direction.INOUT) String filename
   );
    @Method(declaringClass = "onFailure.OnFailureImpl", onFailure=OnFailure.IGNORE)
    void processParam2(
            @Parameter(type = Type.FILE, direction = Direction.INOUT) String filename
   );
}
