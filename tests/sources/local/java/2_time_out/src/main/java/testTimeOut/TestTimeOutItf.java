package testTimeOut;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface TestTimeOutItf {

    @Method(declaringClass = "testTimeOut.TestTimeOutImpl", timeOut = "3", onFailure = OnFailure.IGNORE)
    void timeOutTaskSlow(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Method(declaringClass = "testTimeOut.TestTimeOutImpl", timeOut = "3")
    void timeOutTaskFast(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);
}
