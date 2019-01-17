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
import org.gridlab.gat.io.LogicalFile;


public class LogicalFileAdaptorTest {

    public static void main(String[] args) throws URISyntaxException {
        LogicalFileAdaptorTest a = new LogicalFileAdaptorTest();
        a.test(args[0], args[1].split(",")).print();
        GAT.end();
    }

    public AdaptorTestResult test(String adaptor, String[] hosts)
            throws URISyntaxException {

        if (hosts.length != 4) {
            System.out
                    .println("please provide 4 hosts (comma separated, no spaces)");
            System.exit(1);
        }

        AdaptorTestResult adaptorTestResult = new AdaptorTestResult(adaptor,
                hosts[0]);

        GATContext gatContext = new GATContext();

        Preferences preferences = new Preferences();
        preferences.put("logicalfile.adaptor.name", adaptor);

        LogicalFile logicalFile = null;

        try {
            logicalFile = GAT.createLogicalFile(gatContext, preferences,
                    "test-logical-file", LogicalFile.CREATE);
        } catch (GATObjectCreationException e) {
            e.printStackTrace();
            GAT.end();
            System.exit(1);
        }
        adaptorTestResult.put("replicate [0]", replicateTest(gatContext,
                preferences, logicalFile, new URI("any://" + hosts[0]
                        + "/JavaGAT-test-logical-file"), false));
        adaptorTestResult.put("add       [0]", addTest(gatContext, preferences,
                logicalFile, new URI("any://" + hosts[0]
                        + "/JavaGAT-test-logical-file"), true));
        adaptorTestResult.put("add false [1]", addTest(gatContext, preferences,
                logicalFile, new URI("any://" + hosts[1]
                        + "/JavaGAT-test-logical-file"), false));
        adaptorTestResult.put("replicate [1]", replicateTest(gatContext,
                preferences, logicalFile, new URI("any://" + hosts[1]
                        + "/JavaGAT-test-logical-file"), true));
        adaptorTestResult.put("remove    [1]", removeTest(gatContext,
                preferences, logicalFile, new URI("any://" + hosts[1]
                        + "/JavaGAT-test-logical-file"), true));
        adaptorTestResult.put("add true  [1]", addTest(gatContext, preferences,
                logicalFile, new URI("any://" + hosts[1]
                        + "/JavaGAT-test-logical-file"), true));
        adaptorTestResult.put("replicate2[1]", replicateTest(gatContext,
                preferences, logicalFile, new URI("any://" + hosts[1]
                        + "/JavaGAT-test-logical-file"), false));
        adaptorTestResult.put("replicate [2]", replicateTest(gatContext,
                preferences, logicalFile, new URI("any://" + hosts[2]
                        + "/JavaGAT-test-logical-file"), true));
        adaptorTestResult.put("closest   [3]", closestTest(gatContext,
                preferences, logicalFile, new URI("any://" + hosts[3]
                        + "/JavaGAT-test-logical-file"), true));

        return adaptorTestResult;

    }

    private AdaptorTestResultEntry addTest(GATContext gatContext,
            Preferences preferences, LogicalFile logicalFile, URI toBeAdded,
            boolean expectSuccess) {
        long start = System.currentTimeMillis();
        try {
            logicalFile.addURI(toBeAdded);
        } catch (GATInvocationException e) {
            if (expectSuccess) {
                return new AdaptorTestResultEntry(false, 0L, e);
            }
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry replicateTest(GATContext gatContext,
            Preferences preferences, LogicalFile logicalFile,
            URI toBeReplicated, boolean expectSuccess) {
        long start = System.currentTimeMillis();
        try {
            logicalFile.replicate(toBeReplicated);
        } catch (GATInvocationException e) {
            if (expectSuccess) {
                return new AdaptorTestResultEntry(false, 0L, e);
            }
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry removeTest(GATContext gatContext,
            Preferences preferences, LogicalFile logicalFile, URI toBeRemoved,
            boolean expectSuccess) {
        long start = System.currentTimeMillis();
        try {
            logicalFile.removeURI(toBeRemoved);
        } catch (GATInvocationException e) {
            if (expectSuccess) {
                return new AdaptorTestResultEntry(false, 0L, e);
            }
        } catch (IOException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

    private AdaptorTestResultEntry closestTest(GATContext gatContext,
            Preferences preferences, LogicalFile logicalFile, URI toBeChecked,
            boolean expectSuccess) {
        long start = System.currentTimeMillis();
        try {
            logicalFile.getClosestURI(toBeChecked);
        } catch (GATInvocationException e) {
            if (expectSuccess) {
                return new AdaptorTestResultEntry(false, 0L, e);
            }
        }
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);
    }

}
