/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.nio;

import es.bsc.comm.Connection;
import es.bsc.comm.MessageHandler;
import es.bsc.comm.exceptions.CommException;
import es.bsc.comm.exceptions.CommException.ErrorType;
import es.bsc.comm.nio.exceptions.NIOException;
import es.bsc.comm.stage.Transfer;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.commands.Command;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NIOMessageHandler implements MessageHandler {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    private final NIOAgent agent;


    /**
     * Instantiates a NIO Message Handler associated to a given NIOAgent {@code agent}.
     * 
     * @param agent Associated NIO Agent.
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

        String errorText =
            "NIO Error: " + ce.getMessage() + " processing " + ((t == null) ? "null" : t.hashCode()) + "\n";
        LOGGER.error(errorText, ce);
        boolean managed = this.agent.checkAndHandleRequestedDataNotAvailableError(c);
        if (!managed) {
            managed = this.agent.checkAndHandleCommandError(c);
            if (!managed) {
                this.agent.unhandeledError(c);
            }
        }

    }

    @Override
    public void dataReceived(Connection c, Transfer t) {
        LOGGER.debug(
            "Received data " + (t.isFile() ? t.getFileName() : t.getObject()) + " through connection " + c.hashCode());
        if (t.isArray()) {
            this.agent.receivedPartialBindingObjects(c, t);
        } else {
            this.agent.receivedData(c, t);
        }
    }

    @Override
    public void commandReceived(Connection c, Transfer t) {
        try {
            Command cmd = (Command) t.getObject();
            LOGGER.debug("Received Command " + cmd + " through connection " + c.hashCode());
            cmd.handle(this.agent, c);
        } catch (Exception e) {
            LOGGER.error("Error receving command. Finishing connection.", e);
            c.finishConnection();
        }
    }

    @Override
    public void writeFinished(Connection c, Transfer t) {
        LOGGER.debug("Finished sending " + (t.isFile() ? t.getFileName() : t.getObject()) + " through connection "
            + c.hashCode());
        this.agent.releaseSendSlot(c);
    }

    @Override
    public void connectionFinished(Connection c) {
        LOGGER.debug("Connection " + c.hashCode() + " finished");
        if (!c.hasErrors()) {
            this.agent.unregisterConnectionInOngoingCommands(c);
        }
    }

    @Override
    public void shutdown() {
        // Nothing to do
    }

}
