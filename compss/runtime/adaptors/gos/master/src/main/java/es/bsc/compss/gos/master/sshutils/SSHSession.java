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
package es.bsc.compss.gos.master.sshutils;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.util.Collection;
import java.util.HashSet;


public class SSHSession {

    private final SSHSessionManager sessionManager;
    private Session session;
    private final HashSet<SSHChannel> openChannels;
    private final int maxChannels;
    private static int Created = 1;
    final String id;


    /**
     * Instantiates a new Ssh session.
     *
     * @param s the s
     * @param maxChannels the max channels
     */
    public SSHSession(SSHSessionManager sm, Session s, int maxChannels) {
        this.sessionManager = sm;
        this.session = s;
        this.id = (Created++) + "_" + sm.getHost().getFullHostName();
        setConfig();
        this.maxChannels = Math.max(maxChannels, 1);
        this.openChannels = new HashSet<>(maxChannels);
    }

    private void setConfig() {
        session.setConfig("StrictHostKeyChecking", "no");
        session.setConfig("PreferredAuthentications", "publickey");

    }

    /**
     * Connect session if it is not already connected.
     */
    public void connect() throws JSchException {
        if (!session.isConnected()) {
            try {
                session.connect();
            } catch (JSchException e) {
                if (e.getLocalizedMessage().equals("Packet corrupt")) {
                    recreateSession();
                    session.connect();
                } else {
                    throw e;
                }
            }
        }
    }

    private void recreateSession() throws JSchException {
        session.disconnect();
        session = sessionManager.recreateSession();
        setConfig();
    }

    /**
     * Updates the open channels, the channel must be disconnected in SSHChannel.
     *
     * @param ch the ch
     */
    public void updateOpenChannels(SSHChannel ch) {
        openChannels.remove(ch);
        sessionManager.updateOpenSession(this);
    }

    /**
     * Release all resources.
     */
    public void releaseAllResources() {
        for (SSHChannel ch : openChannels) {
            ch.releaseResources();
        }
        releaseResource();
        openChannels.clear();
    }

    private void releaseResource() {
        session.disconnect();
    }

    /**
     * Open channel boolean.
     *
     * @param type the type
     * @return the boolean
     * @throws JSchException the j sch exception
     */
    public SSHChannel openChannel(String type, String reason) throws JSchException {
        SSHChannel ch = new SSHChannel(this, type, reason);
        openChannels.add(ch);
        return ch;
    }

    public Session getSession() {
        return session;
    }

    public boolean canAddChannel() {
        return openChannels.size() < maxChannels;
    }

    public boolean isEmpty() {
        return openChannels.isEmpty();
    }

    public Collection<? extends SSHChannel> getChannels() {
        return openChannels;
    }

    public SSHHost getHost() {
        return sessionManager.getHost();
    }

    public boolean isConnected() {
        return session.isConnected();
    }

    /**
     * Recreate channel ssh channel.
     *
     * @param old the old
     * @param type the type
     * @return the ssh channel
     * @throws JSchException the j sch exception
     */
    public SSHChannel recreateChannel(SSHChannel old, String type) throws JSchException {
        SSHChannel ch = openChannel(type, old.reason);
        openChannels.remove(old);
        openChannels.add(ch);
        return ch;
    }
}
