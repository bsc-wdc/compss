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

import java.io.File;
import java.io.IOException;

import org.newsclub.net.unix.AFUNIXSelectorProvider;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.newsclub.net.unix.AFUNIXSocketChannel;


/**
 * Base class for socket clients that connect to the runtime UNIX-domain socket server. Concrete implementations only
 * need to provide behaviour for {@link #onConnectionClose()} and message handling.
 */
public abstract class Client extends ConnectionHandler {

    /** Socket endpoint used to connect to the runtime server. */
    private final File socketFile;


    /**
     * Creates a client instance using the provided socket file path.
     *
     * @param socketPath path to the UNIX-domain socket file.
     */
    protected Client(final String socketPath) {
        this(new File(socketPath));
    }

    /**
     * Creates a client instance using the provided socket file.
     *
     * @param endpoint UNIX-domain socket file.
     */
    protected Client(final File endpoint) {
        super();
        this.socketFile = endpoint;
    }

    /**
     * Establishes a non-blocking connection to the runtime socket server. Registers the channel with the listener.
     *
     * @throws IOException when the AFUNIX channel cannot be opened or connected.
     */
    public final void establish() throws IOException {
        AFUNIXSelectorProvider provider = AFUNIXSelectorProvider.provider();
        AFUNIXSocketChannel channel = provider.openSocketChannel();
        channel.configureBlocking(false);
        AFUNIXSocketAddress address = AFUNIXSocketAddress.of(this.socketFile);
        channel.connect(address);
        IPC.registerClient(channel, this, false);
    }

    /**
     * Called when the connection is closed. Removes the client from the listener and triggers a clean-up hook.
     */
    public final void onClose() {
        IPC.deregisterClient(this);
        onConnectionClose();
    }

    /**
     * Returns the UNIX-domain socket file used for the connection.
     *
     * @return socket file reference.
     */
    public final File getSocketFile() {
        return this.socketFile;
    }

    /**
     * Hook invoked once the connection has been closed and deregistered.
     */
    public abstract void onConnectionClose();
}
