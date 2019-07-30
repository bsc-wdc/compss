package testPSCO;

import model.Person;
import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface InternalItf {

    @Method(declaringClass = "testPSCO.InternalImpl")
    public void taskPSCOIn(@Parameter(type = Type.OBJECT, direction = Direction.IN) Person p);

    @Method(declaringClass = "testPSCO.InternalImpl")
    @Constraints(processorName = "MainProcessor2")
    public void taskPSCOInOut(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Person p);

    @Method(declaringClass = "testPSCO.InternalImpl")
    public String taskPSCOInOutTaskPersisted(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Person p);

    @Method(declaringClass = "testPSCO.InternalImpl")
    @Constraints(processorName = "MainProcessor1")
    public Person taskPSCOReturn(@Parameter(type = Type.STRING, direction = Direction.IN) String name,
        @Parameter() int age, @Parameter() int numC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String id);

    @Method(declaringClass = "testPSCO.InternalImpl")
    public Person taskPSCOReturnNoTaskPersisted(@Parameter(type = Type.STRING, direction = Direction.IN) String name,
        @Parameter() int age, @Parameter() int numC);

    @Method(declaringClass = "model.Person")
    public void taskPSCOTarget();

    @Method(declaringClass = "model.Person")
    public void taskPSCOTargetTaskPersisted(@Parameter(type = Type.STRING, direction = Direction.IN) String id);

    @Method(declaringClass = "testPSCO.InternalImpl")
    public Person taskMap(@Parameter(type = Type.STRING, direction = Direction.IN) String newName,
        @Parameter(type = Type.OBJECT, direction = Direction.IN) Person p2);

    @Method(declaringClass = "testPSCO.InternalImpl")
    @Constraints(processorName = "MainProcessor1")
    public Person taskReduce(@Parameter(type = Type.OBJECT, direction = Direction.IN) Person p1,
        @Parameter(type = Type.OBJECT, direction = Direction.IN) Person p2);

    @Method(declaringClass = "model.Person")
    public void taskMap(@Parameter(type = Type.STRING, direction = Direction.IN) String newName);

    @Method(declaringClass = "model.Person")
    public void taskReduce(@Parameter(type = Type.OBJECT, direction = Direction.IN) Person p2);
}
