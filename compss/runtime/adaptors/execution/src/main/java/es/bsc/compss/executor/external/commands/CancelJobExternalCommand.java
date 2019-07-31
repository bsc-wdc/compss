package es.bsc.compss.executor.external.commands;

public class CancelJobExternalCommand implements ExternalCommand {

    @Override
    public CommandType getType() {
        return CommandType.CANCEL_TASK;
    }

    @Override
    public String getAsString() {
        return CommandType.CANCEL_TASK.name();
    }
}
