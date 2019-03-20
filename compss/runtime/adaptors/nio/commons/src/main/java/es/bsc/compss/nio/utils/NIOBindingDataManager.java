/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.comm.nio.NIOConnection;
import es.bsc.comm.stage.Transfer;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.util.BindingDataManager;


public class NIOBindingDataManager extends BindingDataManager {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final Map<NIOConnection, NIOBindingObjectTransferListener> LISTENERS = new ConcurrentHashMap<NIOConnection, NIOBindingObjectTransferListener>();

    static {
        System.loadLibrary("bindings_common");
    }


    public native static int sendNativeObject(String id, NIOBindingObjectStream nioStrm);

    public native static int receiveNativeObject(String id, int type, NIOBindingObjectStream nioStream);

    public static void receiveBindingObject(NIOAgent agent, NIOConnection c, String sourceId, int type) {
        Semaphore sem = new Semaphore(0);
        NIOBindingObjectTransferListener nbol = new NIOBindingObjectTransferListener(agent, sem);
        LISTENERS.put(c, nbol);

        NIOBindingObjectStream nbos = new NIOBindingObjectStream((NIOConnection) c, nbol);
        NIOBindingObjectReceiver receiver = new NIOBindingObjectReceiver(c, sourceId, type, nbos);
        Thread t = new Thread(receiver);
        t.setName("BindingObjectReceiver_" + sourceId);
        t.start();
    }

    public static void receivedPartialBindingObject(NIOConnection c, Transfer t) {
        NIOBindingObjectTransferListener nbol = LISTENERS.get(c);
        nbol.setTransfer(t);
        nbol.notifyEnd();
    }

    public static void objectReceived(NIOConnection c) {
        NIOBindingObjectTransferListener nbol = LISTENERS.remove(c);
        nbol.getAgent().receivedData(c, nbol.getTransfer());
    }

}
