package integratedtoolkit.gat.master.utils;

import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.exceptions.NonInstantiableException;

import java.io.IOException;
import java.util.HashSet;


/**
 * Supporting class to manage SSH connections
 * 
 *
 */
public class SSHManager {

    private static HashSet<COMPSsWorker> workers = new HashSet<>();


    /**
     * Private constructor to avoid instantiation
     */
    private SSHManager() {
        throw new NonInstantiableException("SSHManager");
    }

    /**
     * Registers a new worker
     * 
     * @param worker
     */
    public static void registerWorker(COMPSsWorker worker) {
        synchronized (workers) {
            workers.add(worker);
        }
    }

    /**
     * Removes an existing worker
     * 
     * @param worker
     */
    public static void removeWorker(COMPSsWorker worker) {
        synchronized (workers) {
            workers.remove(worker);
        }
    }

    /**
     * Announces a worker creation (inserts ssh keys in all existing nodes)
     * 
     * @param worker
     * @throws IOException
     */
    public static void announceCreation(COMPSsWorker worker) throws IOException {
        int i = 0;
        Process[] p;
        synchronized (workers) {
            p = new Process[workers.size() * 2];

            for (COMPSsWorker remote : workers) {
                String[] cmd = new String[] { "ssh", remote.getUser() + "@" + remote.getName(),
                        "ssh-keyscan -t rsa,dsa " + worker.getName() + " >> /home/" + remote.getUser() + "/.ssh/known_hosts" };
                p[i] = Runtime.getRuntime().exec(cmd);
                i++;
                cmd = new String[] { "ssh", remote.getUser() + "@" + remote.getName(),
                        "ssh-keyscan -t rsa,dsa " + remote.getName() + " >> /home/" + worker.getUser() + "/.ssh/known_hosts" };
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
     * Announces a worker destruction (cleans ssh keys in worker node)
     * 
     * @param worker
     * @throws IOException
     */
    public static void announceDestruction(COMPSsWorker worker) throws IOException {
        int i = 1;

        Process[] p;
        synchronized (workers) {
            p = new Process[workers.size()];

            for (COMPSsWorker remote : workers) {
                String user = remote.getUser();
                String[] cmd = new String[] { "ssh", user + "@" + remote.getName(), "mv /home/" + user + "/.ssh/known_hosts known "
                        + "&& grep -vw " + worker.getName() + " known > /home/" + user + "/.ssh/known_hosts" + "&& rm known" };
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
     * Removes worker keys
     * 
     * @param worker
     * @throws IOException
     */
    public static void removeKey(COMPSsWorker worker) throws IOException {
        String user = System.getProperty("user.name");
        String[] cmd = new String[] { "mv /home/" + user + "/.ssh/known_hosts known " + "&& grep -vw " + worker.getName()
                + " known > /home/" + user + "/.ssh/known_hosts" + "&& rm known" };
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
