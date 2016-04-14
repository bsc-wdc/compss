package tutorial20;

import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.File;
import org.gridlab.gat.monitoring.MetricEvent;
import org.gridlab.gat.monitoring.MetricListener;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.resources.Job.JobState;

public class SubmitJobCallback implements MetricListener {
    public static void main(String[] args) throws Exception {
        new SubmitJobCallback().start(args[0]);
    }

    public void start(String brokerURI) throws GATObjectCreationException,
            URISyntaxException, GATInvocationException {
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/hostname");
        File stdout = GAT.createFile("hostname.txt");
        sd.setStdout(stdout);

        JobDescription jd = new JobDescription(sd);
        ResourceBroker broker = GAT.createResourceBroker(new URI(brokerURI));
        broker.submitJob(jd, this, "job.status");

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
