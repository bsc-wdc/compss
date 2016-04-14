package examples20;

import java.io.IOException;
import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.LogicalFile;

public class LogicalFileExample {

    /**
     * This example shows the use of the LogicalFile object in JavaGAT
     * 
     * This example requires three valid JavaGAT URIs. The first two URIs should
     * point to files which from the user's perspective are identical. The third
     * URI should point to a location where the logical file should be
     * replicated to. The replication will be done from the 'closest' file.
     * 
     * @param args
     *                a String array of size 3 with each element containing a
     *                String representation of a valid URI.
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out
                    .println("\tUsage: bin/run_gat_app examples20.FileExample <location1> <location2> <location3> (where location is a valid JavaGAT URI)\n\n"
                            + "\twhere:\n"
                            + "\t\t<location1> <location2> point to the same file\n"
                            + "\t\tthis example will replicate to <location3> from the closest\n");
            System.exit(1);
        }
        try {
            new LogicalFileExample().start(new URI(args[0]), new URI(args[1]),
                    new URI(args[2]));
        } catch (URISyntaxException e) {
            System.out.println(e);
            System.out
                    .println("\tUsage: bin/run_gat_app examples20.FileExample <location1> <location2> <location3> (where location is a valid JavaGAT URI)\n\n"
                            + "\twhere:\n"
                            + "\t\t<location1> <location2> point to the same file\n"
                            + "\t\tthis example will replicate to <location3> from the closest\n");
            System.exit(1);
        }
        GAT.end();
    }

    public void start(URI uri1, URI uri2, URI uri3) {
        LogicalFile file = null;
        try {
            file = GAT.createLogicalFile("myLogicalFile", LogicalFile.CREATE);
        } catch (GATObjectCreationException e) {
            System.err.println("Failed to create the logical file: " + e);
            return;
        }
        try {
            file.addURI(uri1);
        } catch (GATInvocationException e) {
            System.err.println("Failed to add uri: '" + uri1
                    + "' to logical file: " + e);
            return;
        } catch (IOException e) {
            System.err.println("Failed to add uri: '" + uri1
                    + "' to logical file: " + e);
            return;
        }
        try {
            file.addURI(uri2);
        } catch (GATInvocationException e) {
            System.err.println("Failed to add uri: '" + uri2
                    + "' to logical file: " + e);
            return;
        } catch (IOException e) {
            System.err.println("Failed to add uri: '" + uri2
                    + "' to logical file: " + e);
            return;
        }
        try {
            System.out.println("" + file.getClosestURI(uri3));
        } catch (GATInvocationException e) {
            System.err.println("Failed to retrieve the closest uri for uri: '"
                    + uri3 + "': " + e);
            return;
        }
        try {
            file.replicate(uri3);
        } catch (GATInvocationException e) {
            System.err.println("Failed to replicate to uri: '" + uri3 + "': "
                    + e);
            return;
        } catch (IOException e) {
            System.err.println("Failed to replicate to uri: '" + uri3 + "': "
                    + e);
            return;
        }
    }
}
