package examples20;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;

public class ResourceBrokerStreamingOutExample {

    /**
     * This example shows the use of the ResourceBroker object in JavaGAT.
     * 
     * This example will submit a job to a resource broker at a given location.
     * It then polls the state of the job until it's stopped.
     * 
     * @param args
     *                a String representation of a valid JavaGAT uri that points
     *                to a resource broker
     * 
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out
                    .println("\tUsage: bin/run_gat_app examples20.ResourceBrokerStreamingOutExample <brokerURI> (where location is a valid JavaGAT URI)\n");

            System.exit(1);
        }
        new ResourceBrokerStreamingOutExample().start(args[0]);
        GAT.end();
    }

    public void start(String brokerURI) {
        ResourceBroker broker = null;
        try {
            broker = GAT.createResourceBroker(new URI(brokerURI));
        } catch (GATObjectCreationException e) {
            System.err.println("Failed to create resource broker at location '"
                    + brokerURI + "': " + e);
            return;
        } catch (URISyntaxException e) {
            System.err.println("Wrong uri '" + brokerURI + "': " + e);
            return;
        }
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/usr/bin/env");
        sd.enableStreamingStdout(true);
        JobDescription jd = new JobDescription(sd);
        Job job = null;
        try {
            job = broker.submitJob(jd);
        } catch (GATInvocationException e) {
            System.err.println("Failed to submit the job: " + e);
            return;
        }
        InputStream in = null;
        try {
            in = job.getStdout();
        } catch (GATInvocationException e) {
            System.err.println("Failed to get the stdout stream: " + e);
            return;
        }
        while (true) {
            int i = 0;
            try {
                i = in.read();
            } catch (IOException e) {
                System.err.println("Failed to read: " + e);
                return;
            }
            if (i == -1) {
                break;
            } else {
                System.out.print((char) i);
            }
        }
    }
}
