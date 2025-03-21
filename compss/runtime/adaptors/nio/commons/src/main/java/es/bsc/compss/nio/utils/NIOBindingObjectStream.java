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
package es.bsc.compss.nio.utils;

import es.bsc.comm.nio.NIOConnection;
import es.bsc.comm.stage.Transfer;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.exceptions.BindingObjectTypeException;

import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NIOBindingObjectStream {

    // Logging
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final String DBG_PREFIX = "[NIOBindingObjectStream] ";

    // Attributes
    private final NIOConnection c;
    private final NIOBindingObjectTransferListener ncl;


    /**
     * New NIOBindingObjectStream instance.
     * 
     * @param c Receiving connection.
     * @param ncl Transfer listener.
     */
    public NIOBindingObjectStream(NIOConnection c, NIOBindingObjectTransferListener ncl) {
        this.c = c;
        this.ncl = ncl;
    }

    /**
     * Sends the given bytes through the connection.
     * 
     * @param b Bytes to send.
     */
    public void push(ByteBuffer b) {
        if (ncl == null) {
            if (b != null) {
                if (b.hasArray()) {
                    byte[] bArray = b.array();
                    LOGGER.debug(DBG_PREFIX + "Sending buffer array " + bArray.length);
                    c.sendDataArray(b.array());
                } else {
                    byte[] bArray = new byte[b.limit()];
                    b.get(bArray);
                    LOGGER.debug(DBG_PREFIX + "Sending array " + bArray.length);
                    c.sendDataArray(bArray);
                }
            }
        }
    }

    /**
     * Receives some bytes from the connection.
     * 
     * @return Read bytes.
     * @throws BindingObjectTypeException When the transfer cannot be converted to array.
     */
    public byte[] pull() throws BindingObjectTypeException {
        LOGGER.debug(DBG_PREFIX + "Pulling byte array");
        ncl.addOperation();
        c.receiveDataArray();
        ncl.enable();
        LOGGER.debug(DBG_PREFIX + "Waiting to receive the data array");
        ncl.aquire();
        Transfer t = ncl.getTransfer();
        if (t.isArray()) {
            byte[] bArray = ncl.getTransfer().getArray();
            LOGGER.debug(DBG_PREFIX + "Returning array of " + bArray.length);
            return bArray;
        } else {
            LOGGER.debug(DBG_PREFIX + "Error is not an Array");
            throw new BindingObjectTypeException("Transfer is not an array");
        }
    }

}
