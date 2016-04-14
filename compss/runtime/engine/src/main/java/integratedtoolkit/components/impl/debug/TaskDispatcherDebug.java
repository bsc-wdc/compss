package integratedtoolkit.components.impl.debug;

import integratedtoolkit.components.impl.TaskDispatcher;
import integratedtoolkit.types.request.td.TDRequest;
import integratedtoolkit.types.request.td.debug.TDDebugRequest;


public class TaskDispatcherDebug extends TaskDispatcher {

    AccessProcessorDebug accessProcessor;

    public void setTP(AccessProcessorDebug ap) {
        this.accessProcessor = ap;
        scheduler.setCoWorkers(jobManager);
        jobManager.setCoWorkers(ap, this);

    }

    @Override
    protected void dispatchRequest(TDRequest request) throws Exception {
        if (request.getRequestType() == TDRequest.TDRequestType.DEBUG) {
            TDDebugRequest drequest = (TDDebugRequest) request;
            switch (drequest.getDebugRequestType()) {
                default:
            }
        } else {
            super.dispatchRequest(request);
        }
    }

}
