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

import java.util.Collection;
import java.util.HashMap;


public class SSHGlobalHostCollection {

    private final HashMap<String, SSHHost> hosts = new HashMap<>();


    /**
     * Get host ssh host.
     *
     * @param user the user
     * @param hostname the host
     * @return the ssh host
     */
    public SSHHost getHost(String user, String hostname) {
        String key = user + "@" + hostname;
        if (hosts.containsKey(key)) {
            return hosts.get(key);
        } else {
            return createHost(user, hostname);
        }
    }

    private SSHHost createHost(String user, String hostname) {
        String key = user + "@" + hostname;
        SSHHost sshHost = new SSHHost(user, hostname);
        hosts.put(key, sshHost);
        return sshHost;
    }

    /**
     * Release all given resources of SSHHosts.
     */
    public void releaseAllResources() {
        for (SSHHost host : hosts.values()) {
            host.releaseAllResources();
        }
        hosts.clear();
    }

    /**
     * Gets all sessions.
     *
     * @return the all sessions
     */
    public HashMap<String, Collection<SSHSession>> getAllSessions() {
        HashMap<String, Collection<SSHSession>> res = new HashMap<>();
        for (SSHHost h : hosts.values()) {
            String key = h.getFullHostName();
            Collection<SSHSession> value = h.getSessions();
            res.put(key, value);
        }
        return res;
    }

    /**
     * Gets all channels.
     *
     * @return the all channels
     */
    public HashMap<String, Collection<SSHChannel>> getAllChannels() {
        HashMap<String, Collection<SSHChannel>> res = new HashMap<>();
        for (SSHHost h : hosts.values()) {
            String key = h.getFullHostName();
            Collection<SSHChannel> value = h.getAllChannels();
            res.put(key, value);
        }
        return res;
    }

}
