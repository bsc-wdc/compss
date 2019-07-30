package testCOMPSsExceptions;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface TestCOMPSsExceptionsItf {

    @Method(declaringClass = "testCOMPSsExceptions.TestCOMPSsExceptionsImpl")
    void writeOne(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Method(declaringClass = "testCOMPSsExceptions.TestCOMPSsExceptionsImpl")
    void writeFour(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Method(declaringClass = "testCOMPSsExceptions.TestCOMPSsExceptionsImpl")
    void writeThree(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);
}
