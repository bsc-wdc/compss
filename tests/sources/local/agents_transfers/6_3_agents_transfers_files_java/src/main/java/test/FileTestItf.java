package test;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface FileTestItf {

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void in_return(@Parameter(type = Type.FILE, direction = Direction.IN) String pathA,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String pathC);

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void in_return_w_print(@Parameter(type = Type.FILE, direction = Direction.IN) String pathA,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String pathC);

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_3")
    void nested_in_return(@Parameter(type = Type.FILE, direction = Direction.IN) String pathA,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String pathC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_3")
    void nested_inout(@Parameter(type = Type.FILE, direction = Direction.INOUT) String pathC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void inout(@Parameter(type = Type.FILE, direction = Direction.INOUT) String pathC);

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void inout_w_print(@Parameter(type = Type.FILE, direction = Direction.INOUT) String pathC);

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_3")
    void print_task(@Parameter(type = Type.FILE, direction = Direction.IN) String pathC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_3")
    void nested_generation_return(@Parameter(type = Type.FILE, direction = Direction.OUT) String pathC);

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void generation_return(@Parameter(type = Type.FILE, direction = Direction.OUT) String pathC);

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void consumption(@Parameter(type = Type.FILE, direction = Direction.IN) String pathC,
        @Parameter(type = Type.STRING, direction = Direction.IN) String label);

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_3")
    void nested_generation_inout(@Parameter(type = Type.FILE, direction = Direction.INOUT) String pathC);

    @Method(declaringClass = "test.FileTest")
    @Constraints(processorArchitecture = "processor_ag_2", operatingSystemType = "agent_2")
    void generation_inout(@Parameter(type = Type.FILE, direction = Direction.INOUT) String pathC);

}
