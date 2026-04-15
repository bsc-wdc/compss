/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.NoConnectionPendingException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.newsclub.net.unix.AFUNIXSelectorProvider;

/**
 * Single-threaded selector loop that multiplexes non-blocking UNIX-domain sockets for both servers and clients. It
 * applies deferred registration requests and dispatches lifecycle events to the corresponding {@link ConnectionHandler}
 * instances.
 */
public class NIOListener {

    private static final int PACKET_SIZE = 10_240;
    private static final int NETWORK_BUFFER_SIZE = 150 * PACKET_SIZE;

    private final Selector selector;
    private boolean active;

    private Map<Server, SelectionKey> servers; // Items never removed, no volatile
    private Set<SocketChannel> openChannels; // Private to thread, no volatile
    private BlockingQueue<ChangeRequest> pendingChanges;
    private Map<SocketChannel, ChangeRequest> pendingInterests; // Private to thread, no volatile


    /**
     * Builds a selector-based listener using the {@link AFUNIXSelectorProvider}.
     *
     * @throws IOException if the selector cannot be created.
     */
    public NIOListener() throws IOException {
        this.selector = AFUNIXSelectorProvider.provider().openSelector();
        this.active = true;
        this.servers = new HashMap<>(); // Items never removed, no volatile
        this.openChannels = new HashSet<>();
        this.pendingChanges = new LinkedBlockingQueue<>();
        this.pendingInterests = new HashMap<>();

    }

    /**
     * Starts the event loop. This call blocks until {@link #stop()} is invoked.
     */
    public final void start() {
        listen();
    }

    /** Requests the listener to stop and wakes up the selector so the loop can exit. */
    public final void stop() {
        this.active = false;
        this.selector.wakeup();
    }

    /**
     * Registers a new server to monitor.
     *
     * @param channel non-blocking server socket channel accepting UNIX-domain connections.
     * @param s server instance that will produce {@link ConnectionHandler} objects for accepted clients.
     * @throws IOException if the registration cannot be enqueued.
     */
    public void registerServer(ServerSocketChannel channel, Server s) throws IOException {
        addChangeRequest(new AcceptCR(channel, s));
    }

    /**
     * Deregisters a server so that newly accepted connections are no longer processed.
     *
     * @param s server being removed from the listener.
     */
    public void deregisterServer(Server s) {
        SelectionKey key = this.servers.remove(s);
        if (key != null) {
            key.cancel();
        }
        this.selector.wakeup();
    }

    /**
     * Registers a new client connection so the listener can complete the handshake and propagate I/O events.
     *
     * @param channel client socket channel.
     * @param c connection handler attached to the channel.
     * @param handleConnection {@code true} if the listener must finish the connect operation.
     * @throws IOException if the registration cannot be enqueued.
     */
    public void registerClient(SocketChannel channel, Client c, boolean handleConnection) throws IOException {
        if (handleConnection) {
            addChangeRequest(new ConnectCR(channel, c));
        } else {
            acceptedConnection(channel, c);
        }
    }

    /**
     * Schedules a write interest for the connection represented by the handler.
     *
     * @param handler connection handler emitting the data to send.
     * @throws IOException if the request cannot be enqueued.
     */
    public void writeConnection(ConnectionHandler handler) throws IOException {
        addChangeRequest(new WriteCR(handler.getSocketChannel(), handler));
    }

    /**
     * Requests the closure of the connection managed by the given handler.
     *
     * @param handler connection to deregister and close.
     * @throws IOException if the request cannot be enqueued.
     */
    public void deregisterConnection(ConnectionHandler handler) throws IOException {
        addChangeRequest(new CloseCR(handler.getSocketChannel(), handler));
    }

    /**
     * Runs the selector event loop until {@link #stop()} is invoked.
     */
    public void listen() {
        while (active) {
            try {
                // Do any pending changes
                // NOTE: All modifications to selector should be done in the same thread
                applyInterestChanges();
                // Timeout necessary in case selector.wakeup() is done after synchronized
                // (pendingChanges)
                // and before select()
                int keys = NIOListener.this.selector.select();
                if (keys != 0) {
                    Iterator<SelectionKey> selectedKeys = NIOListener.this.selector.selectedKeys().iterator();
                    // Loop through the ready channels
                    processKeys(selectedKeys);
                }
            } catch (Exception e) {
                System.err.println("ERROR: Exception listening on connection changes");
                e.printStackTrace(System.err);
            }
        }
    }

    /**
     * Processes the set of ready keys returned by the selector, dispatching the corresponding operation.
     *
     * @param selectedKeys iterator over the keys that were selected for I/O.
     * @throws Exception if any handler throws while processing the event.
     */
    private void processKeys(Iterator<SelectionKey> selectedKeys) throws Exception {
        while (selectedKeys.hasNext()) {
            SelectionKey key = selectedKeys.next();
            selectedKeys.remove();
            if (!key.isValid()) {
                System.err.println("WARN: Invalid Key for " + key.channel().hashCode());
                key.cancel();
                continue;
            }

            if (key.isAcceptable()) {
                accept(key);
            } else if (key.isConnectable()) {
                connect(key);
            } else if (key.isReadable()) {
                read(key);
            } else if (key.isWritable()) {
                write(key);
            } else {
                System.err.println(" WARN: Undefined key type for " + key.channel().hashCode());
            }
        }
    }

    private final void accept(SelectionKey key) {
        SocketChannel sc = null;
        try {
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            sc = server.accept();
            sc.configureBlocking(false);
            this.openChannels.add(sc);
            ConnectionHandler handler = null;
            Server s = (Server) key.attachment();
            if (s != null) {
                handler = s.onNewClient();
                handler.established(sc, this);
            }
            addChangeRequest(new ReadCR(sc, handler));

        } catch (Exception e) {
            if (sc != null) {
                try {
                    sc.close();
                } catch (IOException e1) {
                    // Nothing to do
                }
            }
            key.cancel();
        }

    }

    /**
     * Confirms that the connection has been established client -> server. Performs the 3rd step of the handshake.
     *
     * @param key test
     */
    private void connect(SelectionKey key) {
        SocketChannel sc = (SocketChannel) key.channel();
        this.openChannels.add(sc);
        Client client = (Client) key.attachment();
        try {
            try {
                if (sc.finishConnect()) {
                    acceptedConnection(sc, client);
                } else {
                    System.err.println("ERROR: Not finished connection to node " + sc.getRemoteAddress().toString()
                        + "." + " Cancelling key for channel " + sc.hashCode());
                    refusedConnection(sc, null);
                    key.cancel();
                }
            } catch (NoConnectionPendingException npe) {
                // Eventually will be connected
            }
        } catch (Exception e) {
            System.err.println("ERROR: Exception processing connect through channel " + sc.hashCode());
            e.printStackTrace(System.err);
            refusedConnection(sc, e);
            key.cancel();
        }
    }

    /**
     * Finalises the setup for an accepted or already connected channel by switching it to read mode and notifying the
     * handler.
     *
     * @param sc connected socket channel.
     * @param c client handler attached to the channel.
     * @throws IOException if the registration cannot be enqueued.
     */
    private void acceptedConnection(SocketChannel sc, Client c) throws IOException {
        addChangeRequest(new ReadCR(sc, c));
        c.established(sc, this);
    }

    /**
     * Cleans up resources for a connection that could not be established.
     *
     * @param sc socket channel to close.
     * @param e optional exception describing the failure.
     */
    private void refusedConnection(SocketChannel sc, Exception e) {
        // Close socket
        try {
            System.err.println("Closing socket " + sc.hashCode() + " because connection was refused.");
            sc.close();
        } catch (Exception ioe) {
            // Do nothing
        }
    }

    /**
     * Reads available bytes from a channel and forwards them to the associated handler.
     *
     * @param key selection key for the readable channel.
     */
    private void read(SelectionKey key) {
        SocketChannel sc = (SocketChannel) key.channel();
        try {
            // Read data from the socket
            ByteBuffer readBuffer = ByteBuffer.allocate(PACKET_SIZE);
            int size = sc.read(readBuffer);
            if (size == -1) {
                closeChannel(key, sc);
                return;
            }
            // Ask TransferManager to process the data
            readBuffer.flip();

            ConnectionHandler handler = (ConnectionHandler) key.attachment();
            if (handler != null) {
                handler.onMessageReception(readBuffer);
            }
        } catch (Exception ioe) {
            System.err.println("ERROR: Exception reading key in channel " + sc.hashCode());
            closeChannel(key, sc);
        }
    }

    /**
     * Writes pending data to a channel or switches the interest back to read if no data remains.
     *
     * @param key selection key for the writable channel.
     * @throws Exception if the handler fails to supply data or the channel fails while writing.
     */
    private void write(SelectionKey key) throws Exception {
        SocketChannel sc = (SocketChannel) key.channel();
        ConnectionHandler h = (ConnectionHandler) key.attachment();
        // Each wake-up drains a single buffer to keep the selector iteration short and responsive.
        ByteBuffer bb = h.getMessageToSend();
        if (bb == null) {
            // Nothing left to send -> fall back to monitoring reads only.
            addChangeRequest(new ReadCR(sc, h));
            return;
        }

        sc.write(bb);
        if (bb.hasRemaining()) {
            // Channel could not accept the full payload; requeue the same buffer and stay interested in writes.
            h.requeueMessage(bb);
            addChangeRequest(new WriteCR(sc, h));
            return;
        }

        // Peek for more pending buffers: if we find one, push it back to the front and stay on OP_WRITE so the selector
        // wakes us up again immediately. This spreads work across channels while guaranteeing the queue drains.
        ByteBuffer next = h.getMessageToSend();
        if (next != null) {
            // There is more data pending; ensure it is the next buffer processed on the following write-ready pass.
            h.requeueMessage(next);
            addChangeRequest(new WriteCR(sc, h));
        } else {
            // Queue is empty -> go back to read interest until new messages arrive.
            addChangeRequest(new ReadCR(sc, h));
        }

    }

    /**
     * Closes the provided channel, cancels its key, and notifies the attached handler.
     *
     * @param key selection key associated with the channel.
     * @param sc socket channel to close.
     */
    private void closeChannel(SelectionKey key, SocketChannel sc) {
        // Cancel the key
        if (key != null) {
            key.cancel();
        }
        this.openChannels.remove(sc);

        // If the socket is open, request and notify the connection closure and close it
        if (sc.isOpen()) {

            // Request socket closure
            try {
                sc.close();
            } catch (IOException e) {
                System.err.println("Could not close channel " + sc);
            }

            ConnectionHandler handler = (ConnectionHandler) key.attachment();
            if (handler != null) {
                handler.closed();
            }
        }
    }

    /**
     * Adds a change request to the queue so it can be executed on the listener thread.
     *
     * @param cr change request to enqueue.
     * @throws IOException if the request cannot be queued.
     */
    private void addChangeRequest(ChangeRequest cr) throws IOException {
        // This method can fail
        if (!this.pendingChanges.offer(cr)) {
            System.err
                .println("ERROR in Comm " + cr + " has not been added to the pending request queue! Message Lost ");
        }
        this.selector.wakeup();
    }

    /**
     * Applies all pending change requests and interest updates before waiting on the selector.
     *
     * @throws Exception if a change request fails while applying.
     */
    private void applyInterestChanges() throws Exception {
        while (!this.pendingChanges.isEmpty()) {
            ChangeRequest change = this.pendingChanges.poll();
            change.apply();
        }
    }


    /**
     * Base class for deferred selector modifications that must be executed on the listener thread.
     */
    private abstract class ChangeRequest {

        private final Channel channel;


        /**
         * Creates a new change request bound to the given channel.
         *
         * @param socket channel affected by the change.
         */
        public ChangeRequest(Channel socket) {
            this.channel = socket;
        }

        /**
         * Applies the change, potentially interacting with the selector.
         *
         * @throws IOException if the change cannot be executed.
         */
        public abstract void apply() throws IOException;

    }

    /**
     * Change request that registers a server channel for accept operations.
     */
    private class AcceptCR extends ChangeRequest {

        private final Server server;


        private AcceptCR(Channel socket, Server s) {
            super(socket);
            this.server = s;
        }

        public void apply() throws IOException {
            ServerSocketChannel sc = (ServerSocketChannel) super.channel;
            SelectionKey key = ((SelectableChannel) sc).register(NIOListener.this.selector, SelectionKey.OP_ACCEPT);
            key.attach(server);
            if (!key.isValid()) {
                System.err.println("WARN: Key for Channel " + sc.hashCode() + " operation OP_ACCEPT not valid.");
            }
            NIOListener.this.servers.put(server, key);
        }

    }

    /**
     * Change request that registers a client channel to monitor connect completion.
     */
    private class ConnectCR extends ChangeRequest {

        private final Client client;


        private ConnectCR(Channel socket, Client client) {
            super(socket);
            this.client = client;
        }

        public void apply() throws IOException {
            SocketChannel sc = (SocketChannel) super.channel;
            if (sc.isOpen()) {
                SelectionKey key =
                    ((SelectableChannel) sc).register(NIOListener.this.selector, SelectionKey.OP_CONNECT);
                if (!key.isValid()) {
                    System.err.println("WARN: Key for Channel " + sc.hashCode() + " operation OP_CONNECT not valid.");
                }
                key.attach(client);
            } else {
                System.err.println("WARN: Channel " + sc.hashCode() + " is not open!");
            }
        }

    }

    /**
     * Change request that registers a channel for read readiness.
     */
    private class ReadCR extends ChangeRequest {

        private final ConnectionHandler handler;


        private ReadCR(Channel socket, ConnectionHandler handler) {
            super(socket);
            this.handler = handler;
        }

        public void apply() throws IOException {
            SocketChannel sc = (SocketChannel) super.channel;
            if (!sc.isConnected()) {
                NIOListener.this.pendingInterests.put(sc, this);
            } else {
                if (sc.isOpen()) {
                    SelectionKey key =
                        ((SelectableChannel) sc).register(NIOListener.this.selector, SelectionKey.OP_READ);
                    key.attach(this.handler);
                    if (!key.isValid()) {
                        System.err.println("WARN: Key for Channel " + sc.hashCode() + " operation OP_READ not valid.");
                    }
                } else {
                    System.err.println("WARN: Channel " + sc.hashCode() + " is not open!");
                }
            }
        }
    }

    /**
     * Change request that registers a channel for write readiness.
     */
    private class WriteCR extends ChangeRequest {

        private final ConnectionHandler handler;


        private WriteCR(Channel socket, ConnectionHandler handler) {
            super(socket);
            this.handler = handler;
        }

        public void apply() throws IOException {
            SocketChannel sc = (SocketChannel) super.channel;
            if (!sc.isConnected()) {
                NIOListener.this.pendingInterests.put(sc, this);
            } else {
                if (sc.isOpen()) {
                    SelectionKey key =
                        ((SelectableChannel) sc).register(NIOListener.this.selector, SelectionKey.OP_WRITE);
                    key.attach(this.handler);
                    if (!key.isValid()) {
                        System.err.println("WARN: Key for Channel " + sc.hashCode() + " operation OP_WRITE not valid.");
                    }
                    // AFUNIX channels do not support standard socket options; ignore.
                } else {
                    System.err.println("WARN: Channel " + sc.hashCode() + " is not open!");
                }
            }
        }

    }

    /**
     * Change request that ensures a channel is closed and removed from the selector.
     */
    private class CloseCR extends ChangeRequest {

        private final ConnectionHandler handler;


        private CloseCR(Channel socket, ConnectionHandler handler) {
            super(socket);
            this.handler = handler;
        }

        public void apply() {
            SocketChannel sc = (SocketChannel) super.channel;
            // Closure request
            SelectionKey key = sc.keyFor(NIOListener.this.selector);
            // Close Connection
            key.attach(this.handler);
            NIOListener.this.closeChannel(key, sc);
        }

    }
}
