package tutorial20;

import org.gridlab.gat.GAT;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.File;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.Job.JobState;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;

class SubmitJobWithMultipleInputs {
    public static void main(String[] args) throws Exception {
        ResourceBroker broker = GAT.createResourceBroker(new URI(args[0]));
        SoftwareDescription sd = new SoftwareDescription();

        sd.setExecutable(args[1]);
        sd.setStdout(GAT.createFile("stdout.txt"));
        sd.setStderr(GAT.createFile("stderr.txt"));

        String [] arguments = new String[args.length-2];

        for (int i=2;i<args.length;i++) {
            File tmp = GAT.createFile(args[i]);
            sd.addPreStagedFile(tmp);
            arguments[i-2] = tmp.getName();
        }

        sd.setArguments(arguments);

        Job job = broker.submitJob(new JobDescription(sd));

        while ((job.getState() != JobState.STOPPED)
                && (job.getState() != JobState.SUBMISSION_ERROR)) {
            Thread.sleep(1000);
        }

        GAT.end();
    }
}
