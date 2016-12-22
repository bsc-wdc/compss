package blast;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Stream;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.Binary;
import integratedtoolkit.types.annotations.task.Method;


public interface BlastItf {
    
    @Binary(binary = "${BLAST_BINARY}")
    int align(
        @Parameter(type = Type.STRING, direction = Direction.IN) String pParam,
        @Parameter(type = Type.STRING, direction = Direction.IN) String dbParam,
        @Parameter(type = Type.STRING, direction = Direction.IN) String inputFlag,
        @Parameter(type = Type.FILE, direction = Direction.IN) String partitionFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String outputFlag,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String partitionOutput,
        @Parameter(type = Type.STRING, direction = Direction.IN) String extraCMDArgs,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String fileErr
    );

	@Method(declaringClass = "blast.BlastImpl")
	@Constraints(computingUnits = "2")
	void assemblyPartitions(
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String partialFileA,
		@Parameter(type = Type.FILE, direction = Direction.IN) String partialFileB
	);
	
}
