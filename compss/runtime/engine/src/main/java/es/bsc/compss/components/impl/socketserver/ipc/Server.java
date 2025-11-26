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

import java.io.File;
import java.io.IOException;

import org.newsclub.net.unix.AFUNIXSelectorProvider;
import org.newsclub.net.unix.AFUNIXServerSocketChannel;
import org.newsclub.net.unix.AFUNIXSocketAddress;


/**
 * Base class for the runtime UNIX-domain socket server.
 */
public abstract class Server {

    private final File socketFile;
    private AFUNIXServerSocketChannel channel;


    /**
     * Creates a server instance bound to the provided socket file path.
     *
     * @param socketPath path to the UNIX-domain socket.
     */
    protected Server(String socketPath) {
        this(new File(socketPath));
    }

    /**
     * Creates a server instance bound to the provided socket file.
     *
     * @param socketFile UNIX-domain socket file.
     */
    protected Server(File socketFile) {
        this.socketFile = socketFile;
    }

    /**
     * Starts the socket server on the configured UNIX-domain endpoint and registers it with the IPC layer.
     *
     * @throws IOException if the socket cannot be created or bound.
     */
    public final void start() throws IOException {
        if (this.socketFile.exists() && !this.socketFile.delete()) {
            throw new IOException("Cannot remove existing socket file " + this.socketFile);
        }
        File parent = this.socketFile.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("Cannot create directory for socket file " + this.socketFile);
        }
        this.channel = AFUNIXSelectorProvider.provider().openServerSocketChannel();
        this.channel.configureBlocking(false);
        AFUNIXSocketAddress address = AFUNIXSocketAddress.of(this.socketFile);
        this.channel.bind(address);
        IPC.registerServer(this.channel, this);
    }

    /**
     * Stops the server, closing the underlying channel and removing the socket file if present.
     *
     * @throws IOException if the socket file cannot be deleted.
     */
    public final void stop() throws IOException {
        IPC.stopServer(this);
        if (this.channel != null) {
            this.channel.close();
            this.channel = null;
        }
        if (this.socketFile.exists() && !this.socketFile.delete()) {
            throw new IOException("Cannot delete socket file " + this.socketFile);
        }
    }

    /**
     * Returns the UNIX-domain socket file used by the server.
     *
     * @return socket file reference.
     */
    public final File getSocketFile() {
        return this.socketFile;
    }

    /**
     * Creates a connection handler for a newly accepted client.
     *
     * @return handler bound to the client connection.
     */
    public abstract ConnectionHandler onNewClient();

    @Override
    public String toString() {
        return "Server on " + this.socketFile.getAbsolutePath();
    }
}
// CHECKSTYLE:ON
