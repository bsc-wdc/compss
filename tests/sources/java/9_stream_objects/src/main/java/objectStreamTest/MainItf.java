package objectStreamTest;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;
import es.bsc.distrostreamlib.api.objects.ObjectDistroStream;


public interface MainItf {

    @Constraints(processorArchitecture = "Intel")
    @Method(declaringClass = "objectStreamTest.Tasks")
    void writeObjects(@Parameter(type = Type.STREAM, direction = Direction.OUT) ObjectDistroStream<MyObject> ods,
        @Parameter() int sleepTime);

    @Constraints(processorArchitecture = "Intel")
    @Method(declaringClass = "objectStreamTest.Tasks")
    void writeObjectList(@Parameter(type = Type.STREAM, direction = Direction.OUT) ObjectDistroStream<MyObject> ods,
        @Parameter() int sleepTime);

    @Constraints(processorArchitecture = "AMD")
    @Method(declaringClass = "objectStreamTest.Tasks")
    Integer readObjects(@Parameter(type = Type.STREAM, direction = Direction.IN) ObjectDistroStream<MyObject> ods,
        @Parameter() int sleepTime);

    @Constraints(processorArchitecture = "AMD")
    @Method(declaringClass = "objectStreamTest.Tasks")
    Integer readObjectsTimeout(
        @Parameter(type = Type.STREAM, direction = Direction.IN) ObjectDistroStream<MyObject> ods,
        @Parameter() long timeout, @Parameter() int sleepTime);

    @Method(declaringClass = "objectStreamTest.Tasks")
    Integer processObject(@Parameter(type = Type.OBJECT, direction = Direction.IN) MyObject obj);

}