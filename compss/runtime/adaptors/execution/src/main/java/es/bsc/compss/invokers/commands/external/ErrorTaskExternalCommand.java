package es.bsc.compss.invokers.commands.external;

import es.bsc.compss.invokers.external.ExternalCommand;


public class ErrorTaskExternalCommand implements ExternalCommand {

    @Override
    public CommandType getType() {
        return CommandType.ERROR_TASK;
    }

    @Override
    public String getAsString() {
        return CommandType.ERROR_TASK.name();
    }

}