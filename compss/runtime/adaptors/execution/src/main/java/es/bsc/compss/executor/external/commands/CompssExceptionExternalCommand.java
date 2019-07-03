package es.bsc.compss.executor.external.commands;

public class CompssExceptionExternalCommand implements ExternalCommand{
    @Override
    public CommandType getType() {
        return CommandType.COMPSS_EXCEPTION;
    }

    @Override
    public String getAsString() {
        return CommandType.COMPSS_EXCEPTION.name();
    }
}
