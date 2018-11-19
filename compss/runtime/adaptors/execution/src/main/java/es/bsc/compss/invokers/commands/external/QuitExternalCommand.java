package es.bsc.compss.invokers.commands.external;

import es.bsc.compss.invokers.external.ExternalCommand;


public class QuitExternalCommand implements ExternalCommand {

    @Override
    public CommandType getType() {
        return CommandType.QUIT;
    }

    @Override
    public String getAsString() {
        return CommandType.QUIT.name();
    }

}
