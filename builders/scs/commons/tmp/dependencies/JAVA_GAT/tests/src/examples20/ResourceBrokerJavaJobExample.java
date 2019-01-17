package examples20;

import java.net.URISyntaxException;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.resources.JavaSoftwareDescription;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.Job.JobState;

public class ResourceBrokerJavaJobExample {

    /**
     * This example shows the use of the JavaSoftwareDescription in combination
     * with the ResourceBroker object in JavaGAT.
     * 
     * This example will submit a java job to a resource broker at a given
     * location. It then polls the state of the job until it's stopped. Because
     * it has the java option "-version" in it, it will not actually run a java
     * job, but print the java version. Please replace the sd.setXXX parameters,
     * with parameters that resemble a valid java execution (and then remove the
     * "-version").
     * 
     * @param args
     *                a String representation of a valid JavaGAT uri that points
     *                to a resource broker
     * 
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out
                    .println("\tUsage: bin/run_gat_app examples20.ResourceBrokerJavaJobExample <brokerURI> (where location is a valid JavaGAT URI)\n");

            System.exit(1);
        }
        new ResourceBrokerJavaJobExample().start(args[0]);
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
        JavaSoftwareDescription sd = new JavaSoftwareDescription();
        sd.setExecutable("/usr/bin/java");
        sd.setJavaMain("my.package.HelloWorld");
        sd.setJavaArguments("hello", "world");
        sd.setJavaClassPath("myJar1:myDir");
        sd.setJavaOptions("-version");
        sd.addJavaSystemProperty("key", "value");

        try {
            sd.setStdout(GAT.createFile("javajob.txt"));
        } catch (GATObjectCreationException e) {
            System.err
                    .println("Failed to create the stdout file 'javajob.txt': "
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
