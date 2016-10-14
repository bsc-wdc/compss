package graph.objects;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.task.Method;


public interface SparseLUItf {

	@Method(declaringClass = "graph.objects.Block")
    void lu0();

    @Method(declaringClass = "graph.objects.Block")
    void bdiv(
	    @Parameter Block diag
    );

    @Method(declaringClass = "graph.objects.Block")
    void bmod(
	    @Parameter Block row,
	    @Parameter Block col
    );

    @Method(declaringClass = "graph.objects.Block")
    void fwd(
		@Parameter Block diag
    );

    @Method(declaringClass = "graph.objects.Block")
    Block bmodAlloc(
    	@Parameter Block row,
    	@Parameter Block col
     );
    
    @Method(declaringClass = "graph.objects.Block")
    Block initBlock(
    	@Parameter int i,
    	@Parameter int j,
    	@Parameter int N,
    	@Parameter int M
	);
	
}
