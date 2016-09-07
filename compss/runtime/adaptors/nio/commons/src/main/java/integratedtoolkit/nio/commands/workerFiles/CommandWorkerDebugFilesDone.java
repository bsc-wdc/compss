package integratedtoolkit.nio.commands.workerFiles;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;
import integratedtoolkit.nio.commands.Command;


public class CommandWorkerDebugFilesDone extends Command implements Externalizable {

    public CommandWorkerDebugFilesDone() {
        super();
    }

    @Override
    public CommandType getType() {
        return CommandType.GEN_WORKERS_INFO_DONE;
    }

    @Override
    public void handle(Connection c) {
        agent.notifyWorkersDebugInfoGeneration();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public String toString() {
        return "GeneratingWorkerDebugFilesDone";
    }

}
