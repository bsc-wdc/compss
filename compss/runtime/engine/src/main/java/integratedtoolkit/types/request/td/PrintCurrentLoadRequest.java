package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.util.ResourceManager;

/**
 * The DeleteIntermediateFilesRequest represents a request to delete the
 * intermediate files of the execution from all the worker nodes of the resource
 * pool.
 */
public class PrintCurrentLoadRequest extends TDRequest {

    /**
     * Constructs a PrintCurrentLoadRequest
     *
     */
    public PrintCurrentLoadRequest() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        RESOURCES_LOGGER.info(ts.getWorkload().toString());
        ResourceManager.printResourcesState();
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.PRINT_CURRENT_GRAPH;
    }

}
