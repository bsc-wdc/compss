/*
 * Created on Oct 25, 2005
 */
package benchmarks;

import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.FileInterface;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
// import org.gridlab.gat.security.CertificateSecurityContext;
import org.gridlab.gat.security.PasswordSecurityContext;

public class FileAdaptorTest {

    public static void main(String[] args) {
        FileAdaptorTest a = new FileAdaptorTest();
        a.test(args[0], args[1]).print();
        GAT.end();
    }

    public AdaptorTestResult test(String adaptor, String host) {

        run(host, "adaptor-test-init.sh");

        AdaptorTestResult adaptorTestResult = new AdaptorTestResult(adaptor,
                host);

        GATContext gatContext = new GATContext();
        PasswordSecurityContext password = new PasswordSecurityContext(
                "username", "TeMpPaSsWoRd");
        password.addNote("adaptors", "ftp");
        gatContext.addSecurityContext(password);
        // CertificateSecurityContext ctxt = new CertificateSecurityContext(null, null, "username", "passphrase");
        // gatContext.addSecurityContext(ctxt);

        Preferences preferences = new Preferences();
        preferences.put("file.adaptor.name", adaptor + ",local");

        adaptorTestResult.put("exists: absolute existing file", existTest(
                gatContext, preferences, host, "/tmp/JavaGAT-test-exists-file",
                0, true));
        adaptorTestResult.put("exists: absolute existing dir", existTest(
                gatContext, preferences, host, "/tmp/JavaGAT-test-exists-dir",
                0, true));
        adaptorTestResult.put("exists: absolute non-existing file", existTest(
                gatContext, preferences, host, "/tmp/JavaGAT-test-exists-fake",
                0, false));

        adaptorTestResult.put("exists: relative existing file", existTest(
                gatContext, preferences, host, "JavaGAT-test-exists-file", 1,
                true));
        adaptorTestResult.put("exists: relative existing dir", existTest(
                gatContext, preferences, host, "JavaGAT-test-exists-dir", 1,
                true));
        adaptorTestResult.put("exists: relative non-existing file", existTest(
                gatContext, preferences, host, "JavaGAT-test-exists-fake", 1,
                false));

        adaptorTestResult.put("mkdir: absolute non-existing dir", mkdirTest(
                gatContext, preferences, host, "/tmp/JavaGAT-test-mkdir-dir",
                0, true));
        adaptorTestResult.put("mkdir: absolute existing dir", mkdirTest(
                gatContext, preferences, host, "/tmp/JavaGAT-test-mkdir-dir",
                0, false));
        adaptorTestResult.put("mkdir: relative non-existing dir", mkdirTest(
                gatContext, preferences, host, "JavaGAT-test-mkdir-dir", 1,
                true));
        adaptorTestResult.put("mkdir: relative existing dir", mkdirTest(
                gatContext, preferences, host, "JavaGAT-test-mkdir-dir", 1,
                false));

        adaptorTestResult.put("mkdirs: absolute non-existing dir", mkdirsTest(
                gatContext, preferences, host,
                "/tmp/JavaGAT-test-mkdirs/1/2/3", 0, true));
        adaptorTestResult.put("mkdirs: absolute existing dir", mkdirsTest(
                gatContext, preferences, host,
                "/tmp/JavaGAT-test-mkdirs/1/2/3", 0, false));
        adaptorTestResult.put("mkdirs: relative non-existing dir", mkdirsTest(
                gatContext, preferences, host, "JavaGAT-test-mkdirs/1/2/3", 0,
                true));
        adaptorTestResult.put("mkdirs: relative existing dir", mkdirsTest(
                gatContext, preferences, host, "JavaGAT-test-mkdirs/1/2/3", 0,
                false));

        adaptorTestResult.put("delete: absolute existing file", deleteTest(
                gatContext, preferences, host, "/tmp/JavaGAT-test-delete-file",
                0, true));
        adaptorTestResult.put("delete: absolute non-existing file", deleteTest(
                gatContext, preferences, host, "/tmp/JavaGAT-test-delete-file",
                0, false));
        adaptorTestResult.put("delete: absolute existing dir", deleteTest(
                gatContext, preferences, host, "/tmp/JavaGAT-test-delete-dir/",
                0, true));
        adaptorTestResult.put("delete: absolute non-existing dir", deleteTest(
                gatContext, preferences, host, "/tmp/JavaGAT-test-delete-fake",
                0, false));
        adaptorTestResult.put("delete: relative existing file", deleteTest(
                gatContext, preferences, host, "JavaGAT-test-delete-file", 0,
                true));
        adaptorTestResult.put("delete: relative non-existing file", deleteTest(
                gatContext, preferences, host, "JavaGAT-test-delete-file", 0,
                false));
        adaptorTestResult.put("delete: relative existing dir", deleteTest(
                gatContext, preferences, host, "JavaGAT-test-delete-dir", 1,
                true));
        adaptorTestResult.put("delete: relative non-existing dir", deleteTest(
                gatContext, preferences, host, "JavaGAT-test-delete-dir", 1,
                false));

        adaptorTestResult.put("isFile: existing dir", isFileTest(gatContext,
                preferences, host, "JavaGAT-test-filedir-dir", 0, false));
        adaptorTestResult.put("isFile: existing file", isFileTest(gatContext,
                preferences, host, "JavaGAT-test-filedir-file", 0, true));
        adaptorTestResult.put("isFile: non-existing file", isFileTest(
                gatContext, preferences, host, "JavaGAT-test-filedir-fake", 0,
                false));

        adaptorTestResult.put("isDirectory: existing dir", isDirectoryTest(
                gatContext, preferences, host, "JavaGAT-test-filedir-dir", 0,
                true));
        adaptorTestResult.put("isDirectory: existing file", isDirectoryTest(
                gatContext, preferences, host, "JavaGAT-test-filedir-file", 0,
                false));
        adaptorTestResult.put("isDirectory: non-existing file",
                isDirectoryTest(gatContext, preferences, host,
                        "JavaGAT-test-filedir-fake", 0, false));

        adaptorTestResult.put("canRead: readable file", canReadTest(gatContext,
                preferences, host, "JavaGAT-test-mode-readable", 0, true));
        adaptorTestResult.put("canRead: non-readable file", canReadTest(
                gatContext, preferences, host, "JavaGAT-test-mode-unreadable",
                0, false));
        adaptorTestResult.put("canRead: non-existent file", canReadTest(
                gatContext, preferences, host, "JavaGAT-test-mode-does-not-exist",
                0, false));

        adaptorTestResult.put("canWrite: writable file", canWriteTest(
                gatContext, preferences, host, "JavaGAT-test-mode-writable", 0,
                true));
        adaptorTestResult.put("canWrite: non-writable file", canWriteTest(
                gatContext, preferences, host, "JavaGAT-test-mode-unwritable",
                0, false));
        adaptorTestResult.put("canWrite: non-existent file", canWriteTest(
                gatContext, preferences, host, "JavaGAT-test-mode-does-not-exist",
                0, false));

        adaptorTestResult.put("length, existing file:", lengthTest(gatContext, preferences,
                host, "JavaGAT-test-length", 1, 6));
        adaptorTestResult.put("length, non-existent file:", lengthTest(gatContext, preferences,
                host, "JavaGAT-test-length-nonexistent", 1, 0));

        adaptorTestResult.put("list:", listTest(gatContext, preferences, host,
                "JavaGAT-test-list", 2, "file1", "file2", "dir1"));

        adaptorTestResult.put("lastModified, existent file:", lastModifiedTest(gatContext,
                preferences, host, "JavaGAT-test-last-modified", 0,
                458258400000L));

        adaptorTestResult.put("lastModified, non-existent file:", lastModifiedTest(gatContext,
                preferences, host, "JavaGAT-test-last-modified-nonexist", 0,
                0L));

        adaptorTestResult.put("createNewFile: non-existent file",
                createNewFileTest(gatContext, preferences, host,
                        "JavaGAT-test-new-file", 1, true));
        adaptorTestResult.put("createNewFile: existent file",
                createNewFileTest(gatContext, preferences, host,
                        "JavaGAT-test-new-file", 1, false));

        adaptorTestResult.put("copy: small relative file", copyFileTest(
                gatContext, preferences, host, "JavaGAT-test-copy-small", 1,
                true));

        adaptorTestResult.put("copy: large relative file", copyFileTest(
                gatContext, preferences, host, "JavaGAT-test-copy-large", 1,
                true));

        adaptorTestResult.put("copy: small absolute file", copyFileTest(
                gatContext, preferences, host, "/tmp/JavaGAT-test-copy-small",
                1, true));

        adaptorTestResult.put("copy: large absolute file", copyFileTest(
                gatContext, preferences, host, "/tmp/JavaGAT-test-copy-large",
                1, true));

        run(host, "adaptor-test-clean.sh");

        return adaptorTestResult;

    }

    private void run(String host, String script) {
        
        Preferences preferences = new Preferences();
        preferences.put("resourcebroker.adaptor.name", "commandlinessh,sshtrilead,local");
        preferences.put("file.adaptor.name", "commandlinessh,sshtrilead,local");
        
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/bash");
        sd.setArguments(script);
        try {
            sd.addPreStagedFile(GAT.createFile(preferences, "tests" + java.io.File.separator
                    + "src" + java.io.File.separator + "benchmarks"
                    + java.io.File.separator + script));
        } catch (GATObjectCreationException e) {
            e.printStackTrace();
            System.exit(1);
        }
        ResourceBroker broker = null;
        try {
            broker = GAT.createResourceBroker(preferences, new URI("any://"
                    + host));
        } catch (GATObjectCreationException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Job job = null;
        try {
            job = broker.submitJob(new JobDescription(sd));
        } catch (GATInvocationException e) {
            e.printStackTrace();
            System.exit(1);
        }
        while (job.getState() != Job.JobState.STOPPED
                && job.getState() != Job.JobState.SUBMISSION_ERROR) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // ignored
            }
        }
    }

    private AdaptorTestResultEntry existTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            boolean correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long start = System.currentTimeMillis();
        boolean exists;
        try {
            exists = file.exists();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(exists == correctValue,
                (stop - start), null);
    }

    private AdaptorTestResultEntry mkdirTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            boolean correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long start = System.currentTimeMillis();
        boolean mkdir;
        try {
            mkdir = file.mkdir();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(mkdir == correctValue,
                (stop - start), null);
    }

    private AdaptorTestResultEntry mkdirsTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            boolean correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long start = System.currentTimeMillis();
        boolean mkdirs;
        try {
            mkdirs = file.mkdirs();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(mkdirs == correctValue,
                (stop - start), null);
    }

    private AdaptorTestResultEntry deleteTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            boolean correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long start = System.currentTimeMillis();
        boolean delete;
        try {
            delete = file.delete();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(delete == correctValue,
                (stop - start), null);
    }

    private AdaptorTestResultEntry isFileTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            boolean correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        boolean isFile = false;
        long start = System.currentTimeMillis();
        try {
            isFile = file.isFile();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(isFile == correctValue,
                (stop - start), null);
    }

    private AdaptorTestResultEntry isDirectoryTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            boolean correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        boolean isDir = false;
        long start = System.currentTimeMillis();
        try {
            isDir = file.isDirectory();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(isDir == correctValue,
                (stop - start), null);
    }

    private AdaptorTestResultEntry canReadTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            boolean correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long start = System.currentTimeMillis();
        boolean readable;
        try {
            readable = file.canRead();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(readable == correctValue,
                (stop - start), null);
    }

    private AdaptorTestResultEntry canWriteTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            boolean correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long start = System.currentTimeMillis();
        boolean writable;
        try {
            writable = file.canWrite();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(writable == correctValue,
                (stop - start), null);
    }

    private AdaptorTestResultEntry lengthTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            long correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long start = System.currentTimeMillis();
        long length;
        try {
            length = file.length();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(length == correctValue,
                (stop - start), null);
    }

    private AdaptorTestResultEntry listTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            String... correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long start = System.currentTimeMillis();
        String[] list;
        try {
            list = file.list();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(isCorrect(list, correctValue),
                (stop - start), null);
    }

    private static boolean isCorrect(String[] list, String[] correctValue) {
        if (list == null) {
            return correctValue == null;
        }
        // so list is not null
        if (correctValue == null) {
            return false;
        }

        // correct value is also not null
        if (correctValue.length != list.length) {
            return false;
        }
        for (String entry : list) {
            boolean exists = false;
            for (String correctEntry : correctValue) {
                if (entry.equals(correctEntry)) {
                    exists = true;
                    break;
                }
            }
            // if one of the entries doesn't appear in the correct entries it
            // isn't correct.
            if (!exists) {
                return false;
            }
        }
        return true;
    }

    private AdaptorTestResultEntry lastModifiedTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            long correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long start = System.currentTimeMillis();
        long time;
        try {
            time = file.lastModified();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(time == correctValue, (stop - start),
                null);
    }

    private AdaptorTestResultEntry createNewFileTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            boolean correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long start = System.currentTimeMillis();
        boolean created;
        try {
            created = file.createNewFile();
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(created == correctValue,
                (stop - start), null);
    }

    private AdaptorTestResultEntry copyFileTest(GATContext gatContext,
            Preferences preferences, String host, String filename, int tabs,
            boolean correctValue) {
        FileInterface file = null;
        try {
            file = GAT.createFile(gatContext, preferences,
                    "any://" + host + "/" + filename).getFileInterface();
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long start = System.currentTimeMillis();
        try {
            file.copy(new URI("any:///" + filename + ".copy"));
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        boolean exists = new java.io.File(filename + ".copy").exists();
        if (exists) {
            new java.io.File(filename + ".copy").delete();
        }
        return new AdaptorTestResultEntry(exists == correctValue,
                (stop - start), null);
    }

}
