package integratedtoolkit.util;

import integratedtoolkit.types.COMPSsWorker;
import java.util.HashSet;


public class SSHManager {

    private static HashSet<COMPSsWorker> workers = new HashSet<COMPSsWorker>();

    public static void registerWorker(COMPSsWorker worker) {
        synchronized (workers) {
            workers.add(worker);
        }
    }

    public static void removeWorker(COMPSsWorker worker) {
        synchronized (workers) {
            workers.remove(worker);
        }
    }

    public static void announceCreation(COMPSsWorker worker) throws Exception {
        int i = 0;
        Process[] p;
        synchronized (workers) {
            p = new Process[workers.size() * 2];

            for (COMPSsWorker remote : workers) {
                String[] cmd = new String[]{
                    "ssh",
                    remote.getUser() + "@" + remote.getName(),
                    "ssh-keyscan -t rsa,dsa " + worker.getName() + " >> /home/" + remote.getUser() + "/.ssh/known_hosts"
                };
                p[i] = Runtime.getRuntime().exec(cmd);
                i++;
                cmd = new String[]{
                    "ssh",
                    remote.getUser() + "@" + remote.getName(),
                    "ssh-keyscan -t rsa,dsa " + remote.getName() + " >> /home/" + worker.getUser() + "/.ssh/known_hosts"
                };
                p[i] = Runtime.getRuntime().exec(cmd);
                i++;
            }
        }
        i = 0;
        while (i < p.length) {
            p[i].waitFor();
            p[i].getErrorStream().close();
            p[i].getOutputStream().close();
            i++;
        }
    }

    public static void announceDestruction(COMPSsWorker worker) throws Exception {
        int i = 1;

        Process[] p;
        synchronized (workers) {
            p = new Process[workers.size()];

            for (COMPSsWorker remote : workers) {
                String user = remote.getUser();
                String[] cmd = new String[]{
                    "ssh",
                    user + "@" + remote.getName(),
                    "mv /home/" + user + "/.ssh/known_hosts known " + "&& grep -vw "
                    + worker.getName() + " known > /home/" + user + "/.ssh/known_hosts"
                    + "&& rm known"};
                p[i] = Runtime.getRuntime().exec(cmd);
                i++;
            }
        }
        i = 1;
        while (i < p.length) {
            p[i].waitFor();
            p[i].getErrorStream().close();
            p[i].getOutputStream().close();
            i++;
        }

    }

    public static void removeKey(COMPSsWorker worker) throws Exception {
        String user = System.getProperty("user.name");
        String[] cmd = new String[]{"mv /home/" + user + "/.ssh/known_hosts known " + "&& grep -vw "
            + worker.getName() + " known > /home/" + user + "/.ssh/known_hosts"
            + "&& rm known"};
        Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor();
        p.getErrorStream().close();
        p.getOutputStream().close();
    }
}
