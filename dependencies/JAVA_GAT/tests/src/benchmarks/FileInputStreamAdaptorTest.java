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
import org.gridlab.gat.io.FileInputStream;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
// import org.gridlab.gat.security.CertificateSecurityContext;
import org.gridlab.gat.security.PasswordSecurityContext;

public class FileInputStreamAdaptorTest {

    public static void main(String[] args) {
        FileInputStreamAdaptorTest a = new FileInputStreamAdaptorTest();
        a.test(args[0], args[1]).print();
        GAT.end();
    }

    public AdaptorTestResult test(String adaptor, String host) {

        run(host, "fileinputstream-adaptor-test-init.sh");

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
        preferences.put("fileinputstream.adaptor.name", adaptor);
        FileInputStream in = null;
        try {
            in = GAT.createFileInputStream(gatContext, preferences,
                    "any://" + host + "/JavaGAT-test-fileinputstream");
        } catch (GATObjectCreationException e) {
            adaptorTestResult.put("create         ", new AdaptorTestResultEntry(false, 0, e));
            run(host, "fileinputstream-adaptor-test-clean.sh");
            return adaptorTestResult;
        }
        adaptorTestResult.put("markSupported      ", markSupportedTest(in));
        adaptorTestResult
                .put("available:         ", availableTest(in, 0, true));
        adaptorTestResult.put("read: single char a", readTest(in, 'a', true));
        adaptorTestResult.put("read: single char b", readTest(in, 'b', true));
        adaptorTestResult.put("read: single char c", readTest(in, 'c', true));
        adaptorTestResult.put("read: single char !d", readTest(in, 'q', false));
        adaptorTestResult.put("read: small       ", readTest(in, "efg"
                .getBytes(), true));
        byte[] bytes = new byte[1024 * 1024 * 10];
        byte current = 'h';
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = current;
            if (current == 'z') {
                current = '\n';
            } else if (current == '\n') {
                current = 'a';
            } else {
                current++;
            }
        }
        adaptorTestResult.put("read: large          ",
                readTest(in, bytes, true));
        adaptorTestResult.put("skip: small          ", skipTest(in, 100));
        adaptorTestResult.put("skip: large          ", skipTest(in,
                10 * 1024 * 1024));
        adaptorTestResult.put("close                ", closeTest(in));

        run(host, "fileinputstream-adaptor-test-clean.sh");

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

    private AdaptorTestResultEntry availableTest(FileInputStream in,
            long correctValue, boolean correctResult) {
        long start = System.currentTimeMillis();
        boolean correct;
        try {
            correct = in.available() >= correctValue;
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(correct == correctResult,
                (stop - start), null);
    }

    private AdaptorTestResultEntry readTest(FileInputStream in,
            char correctValue, boolean correctResult) {
        long start = System.currentTimeMillis();
        boolean correct;
        try {
            correct = (((char) in.read()) == correctValue);
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(correct == correctResult,
                (stop - start), null);
    }

    private AdaptorTestResultEntry readTest(FileInputStream in,
            byte[] correctValue, boolean correctResult) {
        byte[] result = new byte[correctValue.length];
        int read = 0;
        boolean correct = true;
        long start = System.currentTimeMillis();
        while (read != correctValue.length) {
            try {
                read += in.read(result, read, correctValue.length - read);
            } catch (IOException e) {
                return new AdaptorTestResultEntry(false, 0, e);
            }
        }
        long stop = System.currentTimeMillis();
        boolean printed = false;
        for (int i = 0; i < read; i++) {
            correct = correct && (result[i] == correctValue[i]);
            if (result[i] != correctValue[i] && !printed) {
                System.out.println("i[" + i + "]: result[i]="
                        + (char) result[i] + ", correct[i]="
                        + (char) correctValue[i]);
                printed = true;
            }
        }
        return new AdaptorTestResultEntry(correct, (stop - start), null);
    }

    private AdaptorTestResultEntry skipTest(FileInputStream in, long n) {
        long start = System.currentTimeMillis();
        try {
            long skipped = in.skip(n);
            if (skipped != n) {
                return new AdaptorTestResultEntry(true, System
                        .currentTimeMillis()
                        - start, new Exception("skipped less bytes (" + skipped
                        + ") than correct value (" + n
                        + "), but the correct execution"));
            }
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry markSupportedTest(FileInputStream in) {
        long start = System.currentTimeMillis();
        boolean markSupported = in.markSupported();
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start),
                markSupported ? new Exception("mark supported")
                        : new Exception("mark not supported"));
    }

    private AdaptorTestResultEntry closeTest(FileInputStream in) {
        long start = System.currentTimeMillis();
        try {
            in.close();
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }
}
