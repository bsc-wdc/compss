package integratedtoolkit.nio.commands;

import es.bsc.comm.Connection;
import java.io.Externalizable;

import integratedtoolkit.nio.NIOAgent;

public abstract class Command implements Externalizable {

    // Type of command
    // NEW TASK: send a new task to a node with a list of the files and its locations
    // DATA DEMAND: ask a node for some data
    // DATA NEGATE: can not send the data now
    // DATA RECEIVED: notify the master that the worker has received the data
    // TASK DONE: notify the master that the task has been done
    // SHUTDOWN: tell the worker to shutdown
    // SHUTDOWN: lets the master know that the worker is stopping
	// GEN_TRACE_PACKAGE: Generate Trace package.
	// GEN_TRACE_PACKAGE_DONE: Notification of the end of trace package.
	
    public enum CommandType {
        NEW_TASK, 
        DATA_DEMAND, 
        DATA_NEGATE, 
        DATA_RECEIVED, 
        TASK_DONE, 
        START_WORKER, 
        STOP_WORKER, 
        STOP_WORKER_ACK, 
        GEN_TRACE_PACKAGE, 
        GEN_TRACE_PACKAGE_DONE, 
        GEN_WORKERS_INFO,
        GEN_WORKERS_INFO_DONE,
        CHECK_WORKER, 
        CHECK_WORKER_ACK
    }

    public NIOAgent agent;

    public Command() {
    }

    public Command(NIOAgent agent) {
        this.agent = agent;
    }

    public abstract CommandType getType();

    public abstract void handle(Connection c);

}
