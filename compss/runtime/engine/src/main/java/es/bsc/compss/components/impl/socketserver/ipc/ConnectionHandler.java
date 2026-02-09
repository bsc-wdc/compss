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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingDeque;


/**
 * Base contract for message-oriented handlers that operate on non-blocking socket connections managed by the
 * {@link NIOListener}. Implementations receive lifecycle callbacks as the underlying channel connects, receives data,
 * and eventually closes.
 */
public abstract class ConnectionHandler {

    private SocketChannel sc;
    private NIOListener listener;

    private final LinkedBlockingDeque<ByteBuffer> pendingMsg = new LinkedBlockingDeque<>();


    /**
     * Signals that a socket connection has been established and associated with this handler.
     *
     * @param sc channel representing the live connection.
     * @param listener listener dispatching future I/O events for the channel.
     */
    public final void established(SocketChannel sc, NIOListener listener) {
        this.sc = sc;
        this.listener = listener;
        this.onEstablish();
    }

    /**
     * Returns the socket channel currently managed by the handler.
     *
     * @return active {@link SocketChannel} instance.
     */
    public final SocketChannel getSocketChannel() {
        return this.sc;
    }

    /**
     * Lifecycle hook invoked once the connection has been fully established.
     */
    public abstract void onEstablish();

    /**
     * Invoked when raw data is received on the associated channel.
     *
     * @param msg buffer containing the message payload positioned at the beginning of the readable bytes.
     */
    public abstract void onMessageReception(ByteBuffer msg);

    /**
     * Enqueues a message to be written to the channel. The listener will flush the data asynchronously.
     *
     * @param msg data to send.
     * @throws IOException if the listener cannot schedule the write.
     */
    public final void sendMessage(ByteBuffer msg) throws IOException {
        this.pendingMsg.offer(msg);
        listener.writeConnection(this);
    }

    /**
     * Retrieves the next pending message that should be written to the channel.
     *
     * @return buffer to send or {@code null} if there is no pending data.
     */
    public final ByteBuffer getMessageToSend() {
        return this.pendingMsg.poll();
    }

    /**
     * Requeues a message at the front of the pending list so it is written before newer messages.
     *
     * @param msg buffer that still contains unsent data.
     */
    public final void requeueMessage(ByteBuffer msg) {
        if (msg != null) {
            this.pendingMsg.offerFirst(msg);
        }
    }

    /**
     * Requests to close the connection; the listener will deregister the channel and trigger {@link #onClose()}.
     *
     * @throws IOException if the listener fails to deregister the connection.
     */
    public final void close() throws IOException {
        this.listener.deregisterConnection(this);
    }

    /**
     * Internal callback used by the listener when the channel has been fully closed.
     */
    public final void closed() {
        onClose();
    }

    /**
     * Lifecycle hook invoked after the channel is closed and deregistered.
     */
    public abstract void onClose();
}
// CHECKSTYLE:ON
