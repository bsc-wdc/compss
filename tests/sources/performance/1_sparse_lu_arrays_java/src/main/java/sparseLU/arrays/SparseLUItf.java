package sparseLU.arrays;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface SparseLUItf {

    @Method(declaringClass = "sparseLU.arrays.SparseLUImpl")
    void lu0(
        @Parameter(direction = Direction.INOUT)
        double[] diag
    );

    @Method(declaringClass = "sparseLU.arrays.SparseLUImpl")
    void bdiv(
	    @Parameter
	    double[] diag,
	    @Parameter(direction = Direction.INOUT)
	    double[] row
    );

    @Method(declaringClass = "sparseLU.arrays.SparseLUImpl")
    void bmod(
	    @Parameter
	    double[] row,
	    @Parameter
	    double[] col,
	    @Parameter(direction = Direction.INOUT)
	    double[] inner
    );

    @Method(declaringClass = "sparseLU.arrays.SparseLUImpl")
    void fwd(
		@Parameter
        double[] diag,
        @Parameter(direction = Direction.INOUT)
        double[] col
    );

    @Method(declaringClass = "sparseLU.arrays.SparseLUImpl")
    double[] bmodAlloc(
    	@Parameter
    	double[] row,
    	@Parameter
    	double[] col
     );
    
    @Method(declaringClass = "sparseLU.arrays.SparseLUImpl")
    double[] initBlock(
    	@Parameter
    	int i,
    	@Parameter
    	int j,
    	@Parameter
    	int N,
    	@Parameter
    	int M
	);

}
