package api.calls;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface MainItf {

    @Method(declaringClass = "api.calls.MainImpl")
    void increment(@Parameter(type = Type.FILE, direction = Direction.INOUT) String file);

}
