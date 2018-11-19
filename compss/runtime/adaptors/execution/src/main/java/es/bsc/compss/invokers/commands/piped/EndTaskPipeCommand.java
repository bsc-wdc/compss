package es.bsc.compss.invokers.commands.piped;

import es.bsc.compss.invokers.commands.external.EndTaskExternalCommand;
import es.bsc.compss.invokers.external.piped.PipeCommand;
import es.bsc.compss.invokers.types.ExternalTaskStatus;


public class EndTaskPipeCommand extends EndTaskExternalCommand implements PipeCommand {

    public final Integer jobId;
    public final ExternalTaskStatus taskStatus;


    public EndTaskPipeCommand(String[] line) {
        jobId = Integer.parseInt(line[1]);
        if (line.length > 3) {
            taskStatus = new ExternalTaskStatus(line);
        } else {
            int exitValue = Integer.parseInt(line[2]);
            taskStatus = new ExternalTaskStatus(exitValue);
        }
    }

    public ExternalTaskStatus getTaskStatus() {
        return taskStatus;
    }
}