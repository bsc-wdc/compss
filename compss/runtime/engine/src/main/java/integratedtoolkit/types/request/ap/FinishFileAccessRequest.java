package integratedtoolkit.types.request.ap;

import integratedtoolkit.components.impl.AccessProcessor;
import integratedtoolkit.components.impl.DataInfoProvider;
import integratedtoolkit.components.impl.TaskAnalyser;
import integratedtoolkit.components.impl.TaskDispatcher;
import integratedtoolkit.types.data.AccessParams.FileAccessParams;
import integratedtoolkit.types.request.exceptions.ShutdownException;

public class FinishFileAccessRequest extends APRequest {

	FileAccessParams fap;
	
	public FinishFileAccessRequest(FileAccessParams fap) {
		this.fap = fap;
	}

	@Override
	public APRequestType getRequestType() {
		return APRequestType.FINISH_ACCESS_FILE;
	}

	@Override
	public void process(AccessProcessor ap, TaskAnalyser ta,
			DataInfoProvider dip, TaskDispatcher td) throws ShutdownException {
		dip.finishFileAccess(fap.getMode(), fap.getLocation());
	}

}
