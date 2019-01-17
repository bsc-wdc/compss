/*
 * Created on Oct 25, 2005
 */
package benchmarks;

import java.io.IOException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.io.RandomAccessFile;


public class RandomAccessFileAdaptorTest {

    public static void main(String[] args) {
        RandomAccessFileAdaptorTest a = new RandomAccessFileAdaptorTest();
        a.test(args[0], args[1]).print();
        GAT.end();
    }

    public AdaptorTestResult test(String adaptor, String host) {

        AdaptorTestResult adaptorTestResult = new AdaptorTestResult(adaptor,
                host);

        GATContext gatContext = new GATContext();

        Preferences preferences = new Preferences();
        preferences.put("randomaccessfile.adaptor.name", adaptor);

        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = GAT.createRandomAccessFile(gatContext,
                    preferences,
                    "any://" + host + "/JavaGAT-random-accessfile", "rw");
        } catch (GATObjectCreationException e) {
            e.printStackTrace();
            GAT.end();
            System.exit(1);
        }

        adaptorTestResult.put("length             ", lengthTest(gatContext,
                preferences, randomAccessFile, 0));
        adaptorTestResult.put("write 'lorem ipsum'", writeTest(gatContext,
                preferences, randomAccessFile, "lorem ipsum"));
        adaptorTestResult.put("length after write", lengthTest(gatContext,
                preferences, randomAccessFile, 13));
        adaptorTestResult.put("seek              ", seekTest(gatContext,
                preferences, randomAccessFile, 0));
        adaptorTestResult.put("read              ", readTest(gatContext,
                preferences, randomAccessFile, "lorem ipsum"));
        adaptorTestResult.put("write 'lorem ipsum' 2", writeTest(gatContext,
                preferences, randomAccessFile, "lorem ipsum"));
        adaptorTestResult.put("length after write 2", lengthTest(gatContext,
                preferences, randomAccessFile, 26));
        adaptorTestResult.put("seek 2              ", seekTest(gatContext,
                preferences, randomAccessFile, 0));
        adaptorTestResult.put("read 2              ", readTest(gatContext,
                preferences, randomAccessFile, "lorem ipsum"));
        adaptorTestResult.put("read 3              ", readTest(gatContext,
                preferences, randomAccessFile, "lorem ipsum"));

        return adaptorTestResult;

    }

    private AdaptorTestResultEntry lengthTest(GATContext gatContext,
            Preferences preferences, RandomAccessFile randomAccessFile,
            long correctValue) {
        long start = System.currentTimeMillis();
        long length;
        try {
            length = randomAccessFile.length();
            System.out.println("lenght: " + length);
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(length == correctValue,
                (stop - start), null);
    }

    private AdaptorTestResultEntry writeTest(GATContext gatContext,
            Preferences preferences, RandomAccessFile randomAccessFile,
            String text) {
        long start = System.currentTimeMillis();
        try {
            randomAccessFile.writeUTF(text);
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry seekTest(GATContext gatContext,
            Preferences preferences, RandomAccessFile randomAccessFile,
            long seek) {
        long start = System.currentTimeMillis();
        try {
            randomAccessFile.seek(seek);
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry readTest(GATContext gatContext,
            Preferences preferences, RandomAccessFile randomAccessFile,
            String text) {
        boolean result;
        long start = System.currentTimeMillis();

        try {
            result = randomAccessFile.readUTF().equals(text);
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(result, (stop - start), null);
    }

}
