package es.bsc.compss.types.request.td;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.util.ResourceManager;


/**
 * The DeleteIntermediateFilesRequest represents a request to delete the intermediate files of the execution from all
 * the worker nodes of the resource pool.
 */
public class PrintCurrentLoadRequest extends TDRequest {

    /**
     * Constructs a PrintCurrentLoadRequest
     *
     */
    public PrintCurrentLoadRequest() {
    }

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
