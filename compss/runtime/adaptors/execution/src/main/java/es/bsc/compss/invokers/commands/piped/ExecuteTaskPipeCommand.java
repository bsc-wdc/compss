package es.bsc.compss.invokers.commands.piped;

import es.bsc.compss.invokers.commands.external.ExecuteTaskExternalCommand;
import es.bsc.compss.invokers.external.piped.PipeCommand;


public class ExecuteTaskPipeCommand extends ExecuteTaskExternalCommand implements PipeCommand {

    private final Integer jobId;


    public ExecuteTaskPipeCommand(Integer jobId) {
        this.jobId = jobId;
    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(CommandType.EXECUTE_TASK.name());
        sb.append(TOKEN_SEP);
        sb.append(String.valueOf(jobId));
        for (String c : this.arguments) {
            sb.append(TOKEN_SEP);
            sb.append(c);
        }
        return sb.toString();
    }
}