package integratedtoolkit.nio.commands;

import es.bsc.comm.Connection;
import java.io.Externalizable;

import integratedtoolkit.nio.NIOAgent;

public abstract class Command implements Externalizable {

	
    public enum CommandType {
        NEW_TASK, 					// Send a new task to a node with a list of the files and its locations
        DATA_DEMAND, 				// Ask a node for some data
        DATA_NEGATE, 				// Can not send the data now
        DATA_RECEIVED, 				// Notify the master that the worker has received the data
        TASK_DONE, 					// Notify the master that the task has been done
        START_WORKER, 				// Tell the worker to start
        CHECK_WORKER, 				// Checks if the worker has started
        CHECK_WORKER_ACK,			// Notify the master that the worker has been started
        STOP_WORKER, 				// Tell the worker to shutdown
        STOP_WORKER_ACK, 			// Lets the master know that the worker is stopping
        GEN_TRACE_PACKAGE, 			// Generate Trace package
        GEN_TRACE_PACKAGE_DONE, 	// Notification of the end of trace package
        GEN_WORKERS_INFO,			// Generate worker debug log files
        GEN_WORKERS_INFO_DONE,		// Notification of the end of worker debug log files generation
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
