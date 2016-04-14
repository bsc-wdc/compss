package examples20;

import org.gridlab.gat.GAT;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.URI;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.resources.WrapperJob;
import org.gridlab.gat.resources.WrapperJobDescription;
import org.gridlab.gat.resources.WrapperSoftwareDescription;
import org.gridlab.gat.resources.Job.JobState;

public class ResourceBrokerWrapperJobExample {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        new ResourceBrokerWrapperJobExample().start();
    }

    public void start() throws Exception {
        // First we create a wrapper software description. It describes the
        // executable that's used for the wrapper. We only need to specify the
        // executable, and possibly the stdout and stderr location. The other
        // values (arguments, environment variables, etc.) are automatically
        // filled in by JavaGAT. If there's already a compatible JavaGAT
        // installation on the remote machine, we can use that installation
        // instead of pre staging JavaGAT from the submission machine.
        WrapperSoftwareDescription wsd = new WrapperSoftwareDescription();
        wsd.setStdout(GAT.createFile("wrapper.stdout"));
        wsd.setStderr(GAT.createFile("wrapper.stderr"));
        wsd.setExecutable("/usr/local/package/jdk1.6.0/bin/java");
        wsd.setGATLocation(System.getenv("user.home") + "/JavaGatVersions/JavaGAT-2.0-rc2");

        // Now we create a job description out of the software description.
        WrapperJobDescription wjd = new WrapperJobDescription(wsd);

        // Now we're going to construct 30 wrapped jobs. We add these wrapped
        // jobs to the wrapper job.
        JobDescription[] wrappedJobDescriptions = new JobDescription[30];
        for (int i = 0; i < wrappedJobDescriptions.length; i++) {
            SoftwareDescription sd = new SoftwareDescription();
            sd.setExecutable("/bin/sleep");
            sd.setArguments("" + (int) (30 * Math.random()));
            sd.addPreStagedFile(GAT.createFile("largefile"));
            sd.setStdout(GAT.createFile("stdout." + i));
            Preferences preferences = new Preferences();
            preferences.put("resourcebroker.adaptor.name", "local");
            wrappedJobDescriptions[i] = new JobDescription(sd);
            wjd.add(wrappedJobDescriptions[i], new URI("any://localhost"),
                    preferences);
        }

        // All the wrapped job descriptions are added to the wrapper job
        // description. We're now ready to submit the wrapper job. We create a
        // resource broker, using certain preferences, in order to do this.
        Preferences wrapperPreferences = new Preferences();
        wrapperPreferences.put("resourcebroker.adaptor.name", "globus");
        ResourceBroker broker = GAT.createResourceBroker(wrapperPreferences,
                new URI("any://fs0.das3.cs.vu.nl/jobmanager-sge"));

        // now we're going to submit this wrapper job description 10 times,
        // which will result in 10 wrapper jobs each holding 30 wrapped jobs. We
        // store the wrapper jobs in an array.
        WrapperJob[] wrapperJobs = new WrapperJob[10];
        for (int i = 0; i < wrapperJobs.length; i++) {
            wrapperJobs[i] = (WrapperJob) broker.submitJob(wjd);
        }

        // All the wrapper jobs are submitted. Let's see what happens. We wait
        // until all wrapper jobs are in the state stopped. And in the meantime
        // we print each second the state of the wrapper job and the state of
        // all of its wrapped jobs. (the first entry of a column is the state of
        // the wrapper job, the other entries are states of the wrapped jobs).
        boolean allwrappersstopped = false;
        while (!allwrappersstopped) {
            allwrappersstopped = true;
            for (int i = 0; i < wrapperJobs.length; i++) {
                System.out.print(wrapperJobs[i].getState().toString()
                        .substring(0, 5)
                        + "\t");
                allwrappersstopped = allwrappersstopped
                        & wrapperJobs[i].getState() == JobState.STOPPED;
            }
            System.out.println();
            System.out.println("-----");
            for (int j = 0; j < wrappedJobDescriptions.length; j++) {
                for (int i = 0; i < wrapperJobs.length; i++) {
                    System.out.print(wrapperJobs[i].getJob(
                            wrappedJobDescriptions[j]).getState().toString()
                            .substring(0, 5)
                            + "\t");
                }
                System.out.println();
            }
            System.out.println();
            Thread.sleep(1000);
        }

        GAT.end();

    }
}
