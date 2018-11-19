package es.bsc.compss.invokers.commands.external;

import es.bsc.compss.invokers.external.ExternalCommand;


public class EndTaskExternalCommand implements ExternalCommand {

    @Override
    public CommandType getType() {
        return CommandType.END_TASK;
    }

    @Override
    public String getAsString() {
        return CommandType.END_TASK.name();
    }

}
