package integratedtoolkit.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.comm.Connection;
import es.bsc.comm.MessageHandler;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.exceptions.CommException;
import es.bsc.comm.exceptions.CommException.ErrorType;
import es.bsc.comm.nio.exceptions.NIOException;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.commands.Command;


public class NIOMessageHandler implements MessageHandler {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    private final NIOAgent agent;


    /**
     * Instantiates a NIO Message Handler associated to a given NIOAqgent @agent
     * 
     * @param agent
     */
    public NIOMessageHandler(NIOAgent agent) {
        this.agent = agent;
    }

    @Override
    public void init() throws CommException {
        // The class has all its parameters
        // The server has been initialized by the TransferManager
        // Nothing to do
    }

    @Override
    public void errorHandler(Connection c, Transfer t, CommException ce) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Received error of type " + ce.getError());
            if (ce.getError().equals(ErrorType.NIO)) {
                NIOException ne = (NIOException) ce;
                LOGGER.debug("- NIOException of type " + ne.getSpecificErrorType());
            }
        }

        String errorText = "NIO Error: " + ce.getMessage() + " processing " + ((t == null) ? "null" : t.hashCode()) + "\n";
        LOGGER.error(errorText, ce);

        agent.receivedRequestedDataNotAvailableError(c, t);

        // Handle FINISH_CONNECTION and CLOSED CONNECTION errors
        /*
         * if (((NIOException)ce).getError() == ErrorType.FINISHING_CONNECTION || ((NIOException)ce).getError() ==
         * ErrorType.CLOSED_CONNECTION) { c.finishConnection(); }
         */
    }

    @Override
    public void dataReceived(Connection c, Transfer t) {
        LOGGER.debug("Received data " + (t.isFile() ? t.getFileName() : t.getObject()) + " through connection " + c.hashCode());
        agent.receivedData(c, t);
    }

    @Override
    public void commandReceived(Connection c, Transfer t) {
        try {
            Command cmd = (Command) t.getObject();
            LOGGER.debug("Received Command " + cmd + " through connection " + c.hashCode());
            cmd.setAgent(agent);
            cmd.handle(c);
        } catch (Exception e) {
            LOGGER.error("Error receving command. Finishing connection.", e);
            c.finishConnection();
        }
    }

    @Override
    public void writeFinished(Connection c, Transfer t) {
        LOGGER.debug("Finished sending " + (t.isFile() ? t.getFileName() : t.getObject()) + " through connection " + c.hashCode());
        agent.releaseSendSlot(c);
    }

    @Override
    public void connectionFinished(Connection c) {
        LOGGER.debug("Connection " + c.hashCode() + " finished");
    }

    @Override
    public void shutdown() {
        // Nothing to do
    }

}
