package sparseLU.objects;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.task.Method;


public interface SparseLUItf {

	@Method(declaringClass = "sparseLU.objects.Block")
    void lu0();

    @Method(declaringClass = "sparseLU.objects.Block")
    void bdiv(
	    @Parameter Block diag
    );

    @Method(declaringClass = "sparseLU.objects.Block")
    void bmod(
	    @Parameter Block row,
	    @Parameter Block col
    );

    @Method(declaringClass = "sparseLU.objects.Block")
    void fwd(
		@Parameter Block diag
    );

    @Method(declaringClass = "sparseLU.objects.Block")
    Block bmodAlloc(
    	@Parameter Block row,
    	@Parameter Block col
     );
    
    @Method(declaringClass = "sparseLU.objects.Block")
    Block initBlock(
    	@Parameter int i,
    	@Parameter int j,
    	@Parameter int N,
    	@Parameter int M
	);
	
}
