/*
 * Created on Oct 25, 2005
 */
package benchmarks;

import java.io.IOException;
import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.FileOutputStream;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
// import org.gridlab.gat.security.CertificateSecurityContext;
import org.gridlab.gat.security.PasswordSecurityContext;

public class FileOutputStreamAdaptorTest {

    public static void main(String[] args) {
        FileOutputStreamAdaptorTest a = new FileOutputStreamAdaptorTest();
        a.test(args[0], args[1]).print();
        GAT.end();
    }

    public AdaptorTestResult test(String adaptor, String host) {

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
        preferences.put("fileoutputstream.adaptor.name", adaptor);
        
        FileOutputStream out = null;
        try {
            out = GAT.createFileOutputStream(gatContext, preferences,
                    "any://" + host + "/JavaGAT-test-fileoutputstream");
        } catch (GATObjectCreationException e) {
            adaptorTestResult.put("create         ", new AdaptorTestResultEntry(false, 0, e));
            run(host, "fileoutputstream-adaptor-test-clean.sh");
            return adaptorTestResult;
        }
        byte[] large = new byte[10 * 1024 * 1024];
        for (int i = 0; i < large.length; i++) {
            large[i] = 'a';
        }
        adaptorTestResult.put("write (small)", writeTest(out, "test\n"));
        adaptorTestResult.put("write (large)",
                writeTest(out, new String(large)));
        adaptorTestResult.put("flush        ", flushTest(out));
        adaptorTestResult.put("close        ", closeTest(out));

        run(host, "fileoutputstream-adaptor-test-clean.sh");

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

    private AdaptorTestResultEntry writeTest(FileOutputStream out, String text) {
        long start = System.currentTimeMillis();
        try {
            out.write(text.getBytes());
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry flushTest(FileOutputStream out) {
        long start = System.currentTimeMillis();
        try {
            out.flush();
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry closeTest(FileOutputStream out) {
        long start = System.currentTimeMillis();
        try {
            out.close();
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }
}
