package es.bsc.compss.invokers.commands.piped;

import es.bsc.compss.invokers.commands.external.ErrorTaskExternalCommand;
import es.bsc.compss.invokers.external.piped.PipeCommand;
import es.bsc.compss.invokers.types.ExternalTaskStatus;


public class ErrorTaskPipeCommand extends ErrorTaskExternalCommand implements PipeCommand {

    public ErrorTaskPipeCommand(String[] result) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ExternalTaskStatus getTaskStatus() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
