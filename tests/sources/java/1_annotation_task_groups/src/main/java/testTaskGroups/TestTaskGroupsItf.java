package testTaskGroups;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface TestTaskGroupsItf {
    
    @Method(declaringClass = "testTaskGroups.TestTaskGroupsImpl", timeOut="3000", onFailure=OnFailure.IGNORE)
    void timeOutTaskSlow(
            @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName
    );
    
    @Method(declaringClass = "testTaskGroups.TestTaskGroupsImpl", timeOut="3000")
    void timeOutTaskFast(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName
    );
    
    @Method(declaringClass = "testTaskGroups.TestTaskGroupsImpl")
    void writeTwo(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName
    );
    
    @Method(declaringClass = "testTaskGroups.TestTaskGroupsImpl")
    void writeOne(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName
    );
    
    @Method(declaringClass = "testTaskGroups.TestTaskGroupsImpl")
    void writeFour(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName
    );
    
    @Method(declaringClass = "testTaskGroups.TestTaskGroupsImpl")
    void writeThree(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName
    );
    
    @Method(declaringClass = "testTaskGroups.TestTaskGroupsImpl", onFailure=OnFailure.IGNORE)
    void writeOnFailure(
        @Parameter(type = Type.FILE, direction = Direction.INOUT) String fileName
    );
     
}
