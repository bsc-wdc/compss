package testCommutativeGroups;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface TestCommutativeGroupsItf {

    @Method(declaringClass = "testCommutativeGroups.TestCommutativeGroupsImpl")
    void writeOne(@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Method(declaringClass = "testCommutativeGroups.TestCommutativeGroupsImpl")
    void writeTwoSlow(@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName);

    @Method(declaringClass = "testCommutativeGroups.TestCommutativeGroupsImpl")
    void writeCommutative(@Parameter(type = Type.FILE, direction = Direction.COMMUTATIVE) String fileName,
        @Parameter(type = Type.FILE, direction = Direction.IN) String fileName2,
        @Parameter(type = Type.FILE, direction = Direction.COMMUTATIVE) String fileName3);

    @Method(declaringClass = "testCommutativeGroups.TestCommutativeGroupsImpl")
    int checkContents(@Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName);

    @Method(declaringClass = "testCommutativeGroups.TestCommutativeGroupsImpl")
    void addOneCommutative(@Parameter(type = Type.FILE, direction = Direction.COMMUTATIVE) String fileName);

    @Method(declaringClass = "testCommutativeGroups.TestCommutativeGroupsImpl")
    void accumulateCommutative(@Parameter(type = Type.FILE, direction = Direction.COMMUTATIVE) String fileName,
        @Parameter(type = Type.FILE, direction = Direction.COMMUTATIVE) String fileName2);

    @Method(declaringClass = "testCommutativeGroups.TestCommutativeGroupsImpl")
    void reduce_and_check_task(@Parameter(type = Type.FILE, direction = Direction.COMMUTATIVE) String fileName,
        @Parameter(type = Type.OBJECT, direction = Direction.IN) Integer param);

    @Method(declaringClass = "testCommutativeGroups.TestCommutativeGroupsImpl")
    Integer task(@Parameter(type = Type.INT, direction = Direction.IN) int i);

    @Method(declaringClass = "model.MyFile", targetDirection = Direction.COMMUTATIVE)
    public void writeThree();

    @Method(declaringClass = "model.MyFile", targetDirection = Direction.INOUT)
    public void writeFour();

}
