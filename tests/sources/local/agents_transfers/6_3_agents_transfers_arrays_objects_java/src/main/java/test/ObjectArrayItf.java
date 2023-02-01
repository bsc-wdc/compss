package test;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface ObjectArrayItf {

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    Integer[][] in_return(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer[][] matA);

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    Integer[][] in_return_w_print(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer[][] matA);

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_3")
    Integer[][] nested_in_return(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer[][] matA,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_3")
    void nested_inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Integer[][] matC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Integer[][] matC);

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void inout_w_print(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Integer[][] matC);

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_3")
    void print_task(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer[][] matC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_3")
    Integer[][] nested_generation_return();

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    Integer[][] generation_return();

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void consumption(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer[][] matC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_3")
    void nested_generation_inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Integer[][] matC);

    @Method(declaringClass = "test.ObjectArray")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void generation_inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Integer[][] matC);

}
