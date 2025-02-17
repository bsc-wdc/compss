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

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class SSHSessionManager {

    private static final int CHANNELS_PER_SESSION = 2;
    private static final int MAX_EMPTY_SESSIONS = 3;
    private final JSch jsch;
    private final SSHHost host;
    private final Map<String, SSHSession> openSessions = new ConcurrentHashMap<>();
    private final Map<String, SSHSession> incompleteSessions = new ConcurrentHashMap<>();

    private final Map<String, SSHSession> allSessions = new ConcurrentHashMap<>();


    public SSHSessionManager(SSHHost host) {
        this.host = host;
        this.jsch = host.jsch;
    }

    private void createSession() throws JSchException {
        Session s = jsch.getSession(host.getUser(), host.getHost(), host.getPort());
        SSHSession t = new SSHSession(this, s, CHANNELS_PER_SESSION);
        t.connect();
        openSessions.put(t.id, t);
        incompleteSessions.put(t.id, t);
        allSessions.put(t.id, t);
    }

    /**
     * Open channel channel.
     *
     * @param type the type
     * @return the channel
     */
    public SSHChannel openChannel(String type, String reason) throws JSchException {
        SSHSession[] list = incompleteSessions.values().toArray(new SSHSession[0]);
        for (SSHSession s : list) {
            if (s.canAddChannel()) {
                SSHChannel ch = s.openChannel(type, reason);
                if (!s.canAddChannel()) {
                    incompleteSessions.remove(s.id);
                }
                return ch;
            } else {
                incompleteSessions.remove(s.id);
            }
        }
        createSession();
        return openChannel(type, reason);
    }

    /**
     * Release all resources.
     */
    public void releaseAllResources() {
        incompleteSessions.clear();
        for (SSHSession s : openSessions.values()) {
            s.releaseAllResources();
        }
        openSessions.clear();
        for (SSHSession s : allSessions.values()) {
            if (s.isConnected()) {
                s.releaseAllResources();
            }
        }
    }

    /**
     * Update open session.
     *
     * @param session the session
     */
    public void updateOpenSession(SSHSession session) {
        if (session.canAddChannel()) {
            incompleteSessions.put(session.id, session);
        }
        cleanEmptySessions();
    }

    /**
     * Clean empty sessions.
     */
    public void cleanEmptySessions() {
        int nEmptySessions = 0;
        Object[] objects = openSessions.values().toArray();
        for (Object o : objects) {
            SSHSession s = (SSHSession) o;
            if (s.isEmpty()) {
                nEmptySessions++;
                if (nEmptySessions > MAX_EMPTY_SESSIONS) {
                    s.releaseAllResources();
                    openSessions.remove(s.id);
                    incompleteSessions.remove(s.id);
                }
            }
        }
    }

    public SSHHost getHost() {
        return host;
    }

    public Collection<SSHSession> getSessions() {
        return openSessions.values();
    }

    /**
     * Gets all channels.
     *
     * @return the all channels
     */
    public Collection<SSHChannel> getAllChannels() {
        Collection<SSHChannel> ret = new ArrayList<>();
        for (SSHSession s : openSessions.values()) {
            ret.addAll(s.getChannels());
        }
        return ret;
    }

    public Session recreateSession() throws JSchException {
        return jsch.getSession(host.getUser(), host.getHost(), host.getPort());
    }
}
