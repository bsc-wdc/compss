package sparseLU.files;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface SparseLUItf {

	@Method(declaringClass = "sparseLU.files.SparseLUImpl")
	void lu0(
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String diag
	);
	
	@Method(declaringClass = "sparseLU.files.SparseLUImpl")
    void bdiv(
    	@Parameter(type = Type.FILE) String diag,
    	@Parameter(type = Type.FILE, direction = Direction.INOUT) String row
    );

	@Method(declaringClass = "sparseLU.files.SparseLUImpl")
    void bmod(
    	@Parameter(type = Type.FILE) String row,
    	@Parameter(type = Type.FILE) String col,
    	@Parameter(type = Type.FILE, direction = Direction.INOUT) String inner
    );
	
	@Method(declaringClass = "sparseLU.files.SparseLUImpl")
    void fwd(
    	@Parameter(type = Type.FILE) String a,
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String b
	);

}
