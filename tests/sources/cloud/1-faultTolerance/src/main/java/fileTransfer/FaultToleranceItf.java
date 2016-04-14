package fileTransfer;

import integratedtoolkit.types.annotations.Method;


public interface FaultToleranceItf {

    @Method(declaringClass = "fileTransfer.FaultToleranceImpl")
	void taskLocalhost(
	); 
    
    @Method(declaringClass = "fileTransfer.FaultToleranceImpl")
	void taskHostDown(
	); 
}
