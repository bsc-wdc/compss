package testCommutative;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface TestCommutativeItf {

    @Method(declaringClass = "testCommutative.TestCommutativeImpl")
    void writeOne(@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Method(declaringClass = "testCommutative.TestCommutativeImpl")
    void writeTwoSlow(@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Method(declaringClass = "testCommutative.TestCommutativeImpl")
    void writeCommutative(@Parameter(type = Type.FILE, direction = Direction.IN) String fileName,
        @Parameter(type = Type.FILE, direction = Direction.IN) String fileName2,
        @Parameter(type = Type.FILE, direction = Direction.COMMUTATIVE) String fileName3);

    @Method(declaringClass = "testCommutative.TestCommutativeImpl")
    int checkContents(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Method(declaringClass = "testCommutative.TestCommutativeImpl")
    void addOneCommutative(@Parameter(type = Type.FILE, direction = Direction.COMMUTATIVE) String fileName);

    @Method(declaringClass = "testCommutative.TestCommutativeImpl")
    void accumulateCommutative(@Parameter(type = Type.FILE, direction = Direction.IN) String fileName,
        @Parameter(type = Type.FILE, direction = Direction.COMMUTATIVE) String fileName2);

    @Method(declaringClass = "testCommutative.TestCommutativeImpl")
    void reduce_and_check_task(@Parameter(type = Type.FILE, direction = Direction.COMMUTATIVE) String fileName,
        @Parameter(type = Type.OBJECT, direction = Direction.IN) Integer param);

    @Method(declaringClass = "testCommutative.TestCommutativeImpl")
    Integer task(@Parameter(type = Type.INT, direction = Direction.IN) int i);

    @Method(declaringClass = "model.MyFile", targetDirection = Direction.COMMUTATIVE)
    public void writeThree();

    @Method(declaringClass = "model.MyFile", targetDirection = Direction.INOUT)
    public void writeFour();

}
