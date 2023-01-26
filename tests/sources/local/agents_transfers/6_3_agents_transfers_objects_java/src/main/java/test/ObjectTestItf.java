package test;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface ObjectTestItf {

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    Matrix in_return(@Parameter(type = Type.OBJECT, direction = Direction.IN) Matrix matA);

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    Matrix in_return_w_print(@Parameter(type = Type.OBJECT, direction = Direction.IN) Matrix matA);

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_3")
    Matrix nested_in_return(@Parameter(type = Type.OBJECT, direction = Direction.IN) Matrix matA,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_3")
    void nested_inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Matrix matC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Matrix matC);

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void inout_w_print(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Matrix matC);

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_3")
    void print_task(@Parameter(type = Type.OBJECT, direction = Direction.IN) Matrix matC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_3")
    Matrix nested_generation_return();

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    Matrix generation_return();

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void consumption(@Parameter(type = Type.OBJECT, direction = Direction.IN) Matrix matC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_3")
    void nested_generation_inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Matrix matC);

    @Method(declaringClass = "test.ObjectTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void generation_inout(@Parameter(type = Type.OBJECT, direction = Direction.INOUT) Matrix matC);

}
