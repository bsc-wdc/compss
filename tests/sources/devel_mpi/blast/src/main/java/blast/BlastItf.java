package blast;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.Binary;
import integratedtoolkit.types.annotations.task.Method;


public interface BlastItf {
    
    @Binary(binary = "${BLAST_BINARY}")
    int align(
        @Parameter(type = Type.STRING, direction = Direction.IN) String pFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String pMode,
        @Parameter(type = Type.STRING, direction = Direction.IN) String dFlag,
        @Parameter(type = Type.STRING, direction = Direction.IN) String database,
        @Parameter(type = Type.STRING, direction = Direction.IN) String iFlag,
        @Parameter(type = Type.FILE, direction = Direction.IN) String partitionFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String oFlag,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String partitionOutput,
        @Parameter(type = Type.STRING, direction = Direction.IN) String extraCMDArgs
    );
    
    @Binary(binary = "${BLAST_BINARY}")
    int align(
        @Parameter(type = Type.STRING, direction = Direction.IN) String pFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String pMode,
        @Parameter(type = Type.STRING, direction = Direction.IN) String dFlag,
        @Parameter(type = Type.STRING, direction = Direction.IN) String database,
        @Parameter(type = Type.STRING, direction = Direction.IN) String iFlag,
        @Parameter(type = Type.FILE, direction = Direction.IN) String partitionFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String oFlag,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String partitionOutput
    );
    
	@Method(declaringClass = "blast.BlastImpl")
	@Constraints(computingUnits = "2")
	void assemblyPartitions(
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String partialFileA,
		@Parameter(type = Type.FILE, direction = Direction.IN) String partialFileB
	);
	
}
