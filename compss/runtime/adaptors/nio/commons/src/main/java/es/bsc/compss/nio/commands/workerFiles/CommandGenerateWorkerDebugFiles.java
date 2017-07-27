package es.bsc.compss.nio.commands.workerFiles;

import es.bsc.compss.nio.commands.Command;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;


public class CommandGenerateWorkerDebugFiles extends Command implements Externalizable {

    public CommandGenerateWorkerDebugFiles() {
        super();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
    }

    @Override
    public CommandType getType() {
        return CommandType.GEN_WORKERS_INFO;
    }

    @Override
    public void handle(Connection c) {
        agent.generateWorkersDebugInfo(c);
    }

    @Override
    public String toString() {
        return "GenerateWorkerDebugFiles";
    }

}
