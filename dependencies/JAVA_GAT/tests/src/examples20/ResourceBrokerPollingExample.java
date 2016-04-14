package examples20;

import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.resources.Job.JobState;

public class ResourceBrokerPollingExample {

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
                    .println("\tUsage: bin/run_gat_app examples20.ResourceBrokerPollingExample <brokerURI> (where location is a valid JavaGAT URI)\n");

            System.exit(1);
        }
        new ResourceBrokerPollingExample().start(args[0]);
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
        sd.setExecutable("/bin/hostname");
        try {
            sd.setStdout(GAT.createFile("hostname.txt"));
        } catch (GATObjectCreationException e) {
            System.err
                    .println("Failed to create the stdout file 'hostname.txt': "
                            + e);
            return;
        }
        JobDescription jd = new JobDescription(sd);
        Job job = null;
        try {
            job = broker.submitJob(jd);
        } catch (GATInvocationException e) {
            System.err.println("Failed to submit the job: " + e);
            return;
        }
        while (job.getState() != JobState.STOPPED) {
            System.out.println("job is in state: " + job.getState());
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

}
