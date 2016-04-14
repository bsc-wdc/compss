package examples20;

import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.URI;
import org.gridlab.gat.monitoring.MetricEvent;
import org.gridlab.gat.monitoring.MetricListener;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.resources.Job.JobState;

public class ResourceBrokerCallBackExample implements MetricListener {

    /**
     * This example shows the use of the ResourceBroker object in JavaGAT in
     * combination with callbacks.
     * 
     * This example will submit a job to a resource broker at a given location.
     * It then listens to the "job.status" metric and prints the state changes
     * of the job.
     * 
     * @param args
     *                a String representation of a valid JavaGAT uri that points
     *                to a resource broker
     * 
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out
                    .println("\tUsage: scripts/run-gat-app examples20.ResourceBrokerCallBackExample <brokerURI> (where location is a valid JavaGAT URI)\n");

            System.exit(1);
        }

        new ResourceBrokerCallBackExample().start(args[0]);
        GAT.end();
    }

    public void start(String brokerURI) {
        ResourceBroker broker = null;
        Preferences prefs = new Preferences();

        try {
            broker = GAT.createResourceBroker(prefs, new URI(brokerURI));
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
        try {
            broker.submitJob(jd, this, "job.status");
        } catch (GATInvocationException e) {
            System.err.println("Failed to submit the job: " + e);
            return;
        }
        synchronized (this) {
            try {
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    public void processMetricEvent(MetricEvent event) {
        System.out.println("received state change: " + event.getValue());
        if (event.getValue() == JobState.STOPPED) {
            synchronized (this) {
                notify();
            }
        }

    }
}
