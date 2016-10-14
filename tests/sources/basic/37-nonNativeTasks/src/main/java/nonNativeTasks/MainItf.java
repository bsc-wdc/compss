package nonNativeTasks;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.Binary;
import integratedtoolkit.types.annotations.task.MPI;
import integratedtoolkit.types.annotations.task.Method;
import integratedtoolkit.types.annotations.task.OmpSs;
import integratedtoolkit.types.annotations.task.OpenCL;


public interface MainItf {

    @Method(declaringClass = "nonNativeTasks.MainImpl")
    int normalTask(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message
    );

    @MPI(binary = "${HELLO_WORLD_MPI}", mpiRunner = "mpirun", computingNodes = "1")
    String mpiTask(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message
    );
    
    @OmpSs(binary = "${HELLO_WORLD_OMPSS}")
    String ompssTask(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message
    );
    
    @Binary(binary = "${HELLO_WORLD_BINARY}")
    String binaryTask(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message
    );
    
    @OpenCL(kernel = "${HELLO_WORLD_OPENCL}")
    String openclTask(
        @Parameter(type = Type.STRING, direction = Direction.IN) String message
    );

}
