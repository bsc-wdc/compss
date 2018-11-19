package es.bsc.compss.invokers.commands.external;

import java.util.LinkedList;
import java.util.List;

import es.bsc.compss.invokers.external.ExternalCommand;


public class ExecuteTaskExternalCommand implements ExternalCommand {

    protected final LinkedList<String> arguments = new LinkedList<>();


    @Override
    public CommandType getType() {
        return CommandType.EXECUTE_TASK;
    }

    public void prependArgument(String argument) {
        this.arguments.addFirst(argument);
    }

    public void appendArgument(String argument) {
        this.arguments.add(argument);
    }

    public void appendAllArguments(List<String> arguments) {
        this.arguments.addAll(arguments);
    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(CommandType.EXECUTE_TASK.name());
        for (String c : arguments) {
            sb.append(TOKEN_SEP);
            sb.append(c);
        }
        return sb.toString();
    }
}
