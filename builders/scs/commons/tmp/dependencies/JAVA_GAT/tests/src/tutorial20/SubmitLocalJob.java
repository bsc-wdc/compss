package tutorial20;

import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.File;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.resources.Job.JobState;

public class SubmitLocalJob {
    public static void main(String[] args) throws Exception {
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/hostname");
        File stdout = GAT.createFile("hostname.txt");
        sd.setStdout(stdout);

        JobDescription jd = new JobDescription(sd);
        ResourceBroker broker = GAT.createResourceBroker(new URI(
                "any://localhost"));
        Job job = broker.submitJob(jd);

        while ((job.getState() != JobState.STOPPED)
                && (job.getState() != JobState.SUBMISSION_ERROR))
            Thread.sleep(1000);

        GAT.end();
    }
}
