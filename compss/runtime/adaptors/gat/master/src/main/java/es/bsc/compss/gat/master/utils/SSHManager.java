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
package es.bsc.compss.gat.master.utils;

import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.exceptions.NonInstantiableException;

import java.io.IOException;
import java.util.HashSet;


/**
 * Supporting class to manage SSH connections.
 */
public class SSHManager {

    private static final HashSet<COMPSsWorker> WORKERS = new HashSet<>();


    /**
     * Private constructor to avoid instantiation.
     */
    private SSHManager() {
        throw new NonInstantiableException("SSHManager");
    }

    /**
     * Registers a new worker.
     * 
     * @param worker New worker.
     */
    public static void registerWorker(COMPSsWorker worker) {
        synchronized (WORKERS) {
            WORKERS.add(worker);
        }
    }

    /**
     * Removes an existing worker.
     * 
     * @param worker Worker to remove.
     */
    public static void removeWorker(COMPSsWorker worker) {
        synchronized (WORKERS) {
            WORKERS.remove(worker);
        }
    }

    /**
     * Announces a worker creation (inserts ssh keys in all existing nodes).
     * 
     * @param worker Worker to announce.
     * @throws IOException When the ssh keys cannot be copied or retrieved.
     */
    public static void announceCreation(COMPSsWorker worker) throws IOException {
        int i = 0;
        Process[] p;
        synchronized (WORKERS) {
            p = new Process[WORKERS.size() * 2];

            for (COMPSsWorker remote : WORKERS) {
                String[] cmd = new String[] { "ssh",
                    remote.getUser() + "@" + remote.getName(),
                    "ssh-keyscan -t rsa,dsa " + worker.getName() + " >> /home/" + remote.getUser()
                        + "/.ssh/known_hosts" };
                p[i] = Runtime.getRuntime().exec(cmd);
                i++;
                cmd = new String[] { "ssh",
                    remote.getUser() + "@" + remote.getName(),
                    "ssh-keyscan -t rsa,dsa " + remote.getName() + " >> /home/" + worker.getUser()
                        + "/.ssh/known_hosts" };
                p[i] = Runtime.getRuntime().exec(cmd);
                i++;
            }
        }
        i = 0;
        while (i < p.length) {
            try {
                p[i].waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            p[i].getErrorStream().close();
            p[i].getOutputStream().close();
            i++;
        }
    }

    /**
     * Announces a worker destruction (cleans ssh keys in worker node).
     * 
     * @param worker Worker to destroy.
     * @throws IOException When worker cannot be removed from known_hosts.
     */
    public static void announceDestruction(COMPSsWorker worker) throws IOException {
        int i = 1;

        Process[] p;
        synchronized (WORKERS) {
            p = new Process[WORKERS.size()];

            for (COMPSsWorker remote : WORKERS) {
                String user = remote.getUser();
                String[] cmd = new String[] { "ssh",
                    user + "@" + remote.getName(),
                    "mv /home/" + user + "/.ssh/known_hosts known " + "&& grep -vw " + worker.getName()
                        + " known > /home/" + user + "/.ssh/known_hosts" + "&& rm known" };
                p[i] = Runtime.getRuntime().exec(cmd);
                i++;
            }
        }
        i = 1;
        while (i < p.length) {
            try {
                p[i].waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            p[i].getErrorStream().close();
            p[i].getOutputStream().close();
            i++;
        }

    }

    /**
     * Removes worker keys.
     * 
     * @param worker Worker name.
     * @throws IOException When the worker keys cannot be removed.
     */
    public static void removeKey(COMPSsWorker worker) throws IOException {
        String user = System.getProperty("user.name");
        String execCmd = "mv /home/" + user + "/.ssh/known_hosts known " + "&& grep -vw " + worker.getName()
            + " known > /home/" + user + "/.ssh/known_hosts" + "&& rm known";
        String[] cmd = new String[] { execCmd };
        Process p = Runtime.getRuntime().exec(cmd);
        try {
            p.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        p.getErrorStream().close();
        p.getOutputStream().close();
    }

}
