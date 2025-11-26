/*
 *  Copyright 2023 Francesc Lordan Gomis
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

package es.bsc.compss.components.impl.socketserver.ipc;

import java.io.IOException;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import java.util.LinkedList;
import java.util.List;


/**
 * Coordinates the shared {@link NIOListener} instance and keeps track of the active servers and clients registered
 * against it. The helper ensures the listener thread is started on demand and stops it once no endpoints remain.
 */
public class IPC {

    private static List<Server> activeServers = new LinkedList<>();
    private static List<ConnectionHandler> activeClients = new LinkedList<>();

    private static boolean listenerRunning;
    private static final NIOListener listener;

    static {
        NIOListener l;
        try {
            l = new NIOListener();
        } catch (Exception e) {
            System.out.println("Error starting IPC listener");
            System.exit(1);
            l = null;
        }
        listener = l;
    }


    /**
     * Registers a new server socket with the listener and starts the listener thread if needed.
     *
     * @param channel non-blocking server socket channel already bound to a UNIX-domain address.
     * @param s owner {@link Server} instance used to create connection handlers for accepted clients.
     * @throws IOException if the listener cannot register the channel.
     */
    public static final void registerServer(ServerSocketChannel channel, Server s) throws IOException {
        listener.registerServer(channel, s);
        checkListenerUp();
        activeServers.add(s);
    }

    /**
     * Deregisters the server from the listener and stops the listener thread when no endpoints remain.
     *
     * @param s server that is shutting down.
     */
    public static final void stopServer(Server s) {
        listener.deregisterServer(s);
        activeServers.remove(s);
        if (activeServers.size() + activeClients.size() == 0) {
            checkListenerDown();
        }
    }

    /**
     * Registers a new client connection with the listener.
     *
     * @param channel client socket channel, either already connected or in the middle of the connect handshake.
     * @param c handler responsible for processing events on the client connection.
     * @param handleConnect {@code true} if the listener must complete the connect handshake; {@code false} if the
     *            channel is already connected and ready for I/O.
     * @throws IOException if the listener cannot register the channel.
     */
    public static final void registerClient(SocketChannel channel, Client c, boolean handleConnect) throws IOException {
        checkListenerUp();
        listener.registerClient(channel, c, handleConnect);
        activeClients.add(c);
    }

    /**
     * Removes a client from the listener and stops the listener thread when no endpoints remain.
     *
     * @param c client that no longer requires monitoring.
     */
    public static final void deregisterClient(Client c) {
        activeClients.remove(c);
        if (activeServers.size() + activeClients.size() == 0) {
            checkListenerDown();
        }
    }

    /**
     * Starts the listener thread if it isn't already running.
     */
    private static final void checkListenerUp() {
        synchronized (listener) {
            if (!listenerRunning) {
                listenerRunning = true;
                new Thread() {

                    public void run() {
                        listener.listen();
                        listenerRunning = false;
                    }
                }.start();
            }
        }
    }

    /**
     * Stops the listener when there are no registered clients or servers.
     */
    private static final void checkListenerDown() {
        synchronized (listener) {
            if (listenerRunning) {
                listener.stop();
            }
        }
    }

}
// CHECKSTYLE:ON
