package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.data.AccessParams.FileAccessParams;
import es.bsc.compss.types.request.exceptions.ShutdownException;

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
