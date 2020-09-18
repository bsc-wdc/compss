package testConcurrent;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface TestConcurrentItf {

    @Method(declaringClass = "testConcurrent.TestConcurrentImpl")
    void writeOne(@Parameter(type = Type.FILE, direction = Direction.CONCURRENT) String fileName);

    @Method(declaringClass = "testConcurrent.TestConcurrentImpl")
    void writeTwo(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Method(declaringClass = "model.MyFile", targetDirection = Direction.CONCURRENT)
    public void writeThree();

    @Method(declaringClass = "model.MyFile", targetDirection = Direction.INOUT)
    public void writeFour();

}
