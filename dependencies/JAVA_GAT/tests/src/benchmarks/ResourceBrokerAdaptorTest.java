package benchmarks;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.URI;
import org.gridlab.gat.monitoring.MetricEvent;
import org.gridlab.gat.monitoring.MetricListener;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
// import org.gridlab.gat.security.CertificateSecurityContext;

public class ResourceBrokerAdaptorTest implements MetricListener {

    private boolean jobFinished;
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        ResourceBrokerAdaptorTest a = new ResourceBrokerAdaptorTest();
        a.test(args[0], args[1]).print();
        GAT.end();
    }

    public AdaptorTestResult test(String adaptor, String host) {
        try {
            URI temp = new URI(host);
            if (temp.getScheme() == null && ! host.equals("")) {
                host = "any://" + host;
            }
        } catch (URISyntaxException e) {
            // ignored
        }

        AdaptorTestResult adaptorTestResult = new AdaptorTestResult(adaptor,
                host);
        GATContext gatContext = new GATContext();
        // CertificateSecurityContext ctxt = new CertificateSecurityContext(null, null, "username", "passphrase");
        // gatContext.addSecurityContext(ctxt);
        // Add security contexts to gatContext here.
        Preferences preferences = new Preferences();
        preferences.put("resourcebroker.adaptor.name", adaptor);
        // preferences.put("file.adaptor.name", "commandlinessh,sshtrilead,local");
        jobFinished = false;
        adaptorTestResult.put("submit job easy  ", submitJobEasy(
                gatContext, preferences, host));
        jobFinished = false;
        adaptorTestResult.put("submit job parallel", submitJobParallel(
                gatContext, preferences, host));
        jobFinished = false;
        adaptorTestResult.put("submit job stdout", submitJobStdout(
                gatContext, preferences, host));
        jobFinished = false;
        adaptorTestResult.put("submit job stderr", submitJobStderr(
                gatContext, preferences, host));
        jobFinished = false;
        adaptorTestResult.put("submit job prestage", submitJobPreStage(
                gatContext, preferences, host));
        jobFinished = false;
        adaptorTestResult.put("submit job poststage", submitJobPostStage(
                gatContext, preferences, host));
        jobFinished = false;
        adaptorTestResult.put("submit job environment", submitJobEnvironment(
                gatContext, preferences, host));
        jobFinished = false;
        adaptorTestResult.put("job state consistency",
                submitJobStateConsistency(gatContext, preferences, host));
        jobFinished = false;
        adaptorTestResult.put("job get info        ", submitJobGetInfo(
                gatContext, preferences, host));
        jobFinished = false;
        return adaptorTestResult;
    }
    
    private synchronized void waitForJob() {
        while (! jobFinished) {
            try {
                wait();
            } catch(Throwable e) {
                // ignored
            }
        }
    }

    private AdaptorTestResultEntry submitJobEasy(GATContext gatContext,
            Preferences preferences, String host) {
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/echo");
        sd.setArguments("test", "1", "2", "3");
        Map<String, Object> attributes = new HashMap<String, Object>();

        sd.setAttributes(attributes);
        JobDescription jd = new JobDescription(sd);
        ResourceBroker broker;
        try {
            broker = GAT.createResourceBroker(gatContext, preferences,
                        new URI(host));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        long start = System.currentTimeMillis();
        try {
            broker.submitJob(jd, this, "job.status");
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        waitForJob();
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);

    }

    private AdaptorTestResultEntry submitJobParallel(GATContext gatContext,
            Preferences preferences, String host) {
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/echo");
        sd.setArguments("test", "1", "2", "3");
        try {
            sd.setStdout(GAT.createFile(gatContext, preferences, "parallel-stdout"));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        JobDescription jd = new JobDescription(sd);
        jd.setProcessCount(2);
        jd.setResourceCount(1);
        ResourceBroker broker;
        try {
            broker = GAT.createResourceBroker(gatContext, preferences,
                    new URI(host));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        long start = System.currentTimeMillis();
        try {
            broker.submitJob(jd, this, "job.status");
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        waitForJob();

        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(true, (stop - start), null);

    }

    private AdaptorTestResultEntry submitJobStdout(GATContext gatContext,
            Preferences preferences, String host) {
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/echo");
        // Arguments modified to test against expansion of shell meta characters --Ceriel
        sd.setArguments("test", "1", "2", "'*");
        try {
            sd.setStdout(GAT.createFile(gatContext, preferences, "stdout"));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        JobDescription jd = new JobDescription(sd);
        ResourceBroker broker;
        try {
            broker = GAT.createResourceBroker(gatContext, preferences,
                    new URI(host));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        long start = System.currentTimeMillis();
        Job job;
        try {
            job = broker.submitJob(jd, this, "job.status");
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        waitForJob();
        try {
	    Map<String, Object> info = job.getInfo();
	    Throwable ex = (Throwable) info.get("poststage.exception");
	    if (ex != null) {
		return new AdaptorTestResultEntry(false, 0L, ex);
	    }
	} catch (GATInvocationException e) {
	    return new AdaptorTestResultEntry(false, 0L, e);
	}
        long stop = System.currentTimeMillis();
        String result;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new java.io.FileInputStream("stdout")));
            result = reader.readLine();
            reader.close();
        } catch (Exception e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        return new AdaptorTestResultEntry("test 1 2 '*".equals(result),
                (stop - start), null);

    }

    private AdaptorTestResultEntry submitJobStderr(GATContext gatContext,
            Preferences preferences, String host) {
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/ls");
        sd.setArguments("floep");
        try {
            sd.setStderr(GAT.createFile(gatContext, preferences, "stderr"));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        JobDescription jd = new JobDescription(sd);
        ResourceBroker broker;
        try {
            broker = GAT.createResourceBroker(gatContext, preferences,
                    new URI(host));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        long start = System.currentTimeMillis();
        Job job;
        try {
            job = broker.submitJob(jd, this, "job.status");
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        waitForJob();
        try {
	    Map<String, Object> info = job.getInfo();
	    Throwable ex = (Throwable) info.get("poststage.exception");
	    if (ex != null) {
		return new AdaptorTestResultEntry(false, 0L, ex);
	    }
	} catch (GATInvocationException e) {
	    return new AdaptorTestResultEntry(false, 0L, e);
	}
        long stop = System.currentTimeMillis();
        String result;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new java.io.FileInputStream("stderr")));
            result = reader.readLine();
            reader.close();
        } catch (Exception e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        return new AdaptorTestResultEntry(result != null
                && result.endsWith("floep: No such file or directory"),
                (stop - start), null);

    }

    private AdaptorTestResultEntry submitJobPreStage(GATContext gatContext,
            Preferences preferences, String host) {
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/ls");
        sd.setArguments("floep");
        java.io.File floep = new java.io.File("floep");
        if (!floep.exists()) {
            try {
                floep.createNewFile();
            } catch (IOException e) {
                return new AdaptorTestResultEntry(false, 0L, e);
            }
            floep.deleteOnExit();
        }
        java.io.File tmp = new java.io.File("tmp");
        if (!tmp.exists()) {
                tmp.mkdir();
                tmp.deleteOnExit();
        }
        try {
            sd.addPreStagedFile(GAT.createFile(gatContext, preferences, "floep"));
            sd.addPreStagedFile(GAT.createFile(gatContext, preferences, "tmp"));
            sd.setStdout(GAT.createFile(gatContext, preferences, "stdout"));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        JobDescription jd = new JobDescription(sd);
        ResourceBroker broker;
        try {
            broker = GAT.createResourceBroker(gatContext, preferences,
                    new URI(host));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        long start = System.currentTimeMillis();
        Job job;
        try {
            job = broker.submitJob(jd, this, "job.status");
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        waitForJob();
        try {
	    Map<String, Object> info = job.getInfo();
	    Throwable ex = (Throwable) info.get("poststage.exception");
	    if (ex != null) {
		return new AdaptorTestResultEntry(false, 0L, ex);
	    }
	} catch (GATInvocationException e) {
	    return new AdaptorTestResultEntry(false, 0L, e);
	}
        long stop = System.currentTimeMillis();
        String result;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new java.io.FileInputStream("stdout")));
            result = reader.readLine();
            reader.close();
        } catch (Exception e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        return new AdaptorTestResultEntry(result != null, (stop - start), null);

    }

    private AdaptorTestResultEntry submitJobPostStage(GATContext gatContext,
            Preferences preferences, String host) {
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/touch");
        sd.setArguments("flap.txt");
        try {
            sd.addPostStagedFile(GAT.createFile(gatContext, preferences, "flap.txt"));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        JobDescription jd = new JobDescription(sd);
        ResourceBroker broker;
        try {
            broker = GAT.createResourceBroker(gatContext, preferences,
                    new URI(host));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        long start = System.currentTimeMillis();
        Job job;
        try {
            job = broker.submitJob(jd, this, "job.status");
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        waitForJob();
        try {
	    Map<String, Object> info = job.getInfo();
	    Throwable ex = (Throwable) info.get("poststage.exception");
	    if (ex != null) {
		return new AdaptorTestResultEntry(false, 0L, ex);
	    }
	} catch (GATInvocationException e) {
	    return new AdaptorTestResultEntry(false, 0L, e);
	}
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(
                new java.io.File("flap.txt").exists(), (stop - start), null);

    }

    private AdaptorTestResultEntry submitJobEnvironment(
            GATContext gatContext, Preferences preferences, String host) {
        SoftwareDescription sd = new SoftwareDescription();
        Map<String, Object> env = new HashMap<String, Object>();
        env.put("JAVAGAT_TEST_KEY", "javagat-test-value");
        sd.setEnvironment(env);
        sd.setExecutable("/usr/bin/env");
        try {
            sd.setStdout(GAT.createFile(gatContext, preferences, "stdout"));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        JobDescription jd = new JobDescription(sd);
        ResourceBroker broker;
        try {
            broker = GAT.createResourceBroker(gatContext, preferences,
                    new URI(host));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        long start = System.currentTimeMillis();
        Job job;
        try {
            job = broker.submitJob(jd, this, "job.status");
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        waitForJob();
        try {
	    Map<String, Object> info = job.getInfo();
	    Throwable ex = (Throwable) info.get("poststage.exception");
	    if (ex != null) {
		return new AdaptorTestResultEntry(false, 0L, ex);
	    }
	} catch (GATInvocationException e) {
	    return new AdaptorTestResultEntry(false, 0L, e);
	}
        long stop = System.currentTimeMillis();
        boolean success = false;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new java.io.FileInputStream("stdout")));
            while (true) {
                String result = reader.readLine();
                if (result == null) {
                    break;
                }
                if (result.contains("JAVAGAT_TEST_KEY")
                        && result.contains("javagat-test-value")) {
                    success = true;
                }
            }
            reader.close();
        } catch (Exception e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        return new AdaptorTestResultEntry(success, (stop - start), null);

    }

    private AdaptorTestResultEntry submitJobStateConsistency(
            GATContext gatContext, Preferences preferences, String host) {
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/sleep");
        sd.setArguments("2");
        JobDescription jd = new JobDescription(sd);
        ResourceBroker broker;
        try {
            broker = GAT.createResourceBroker(gatContext, preferences,
                    new URI(host));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        JobStateMetricListener listener = new JobStateMetricListener(this);
        long start = System.currentTimeMillis();
        try {
            broker.submitJob(jd, listener, "job.status");
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        waitForJob();
        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(listener.getException() == null,
                (stop - start), listener.getException());

    }

    class JobStateMetricListener implements MetricListener {

        private Job.JobState state = Job.JobState.INITIAL;

        private Exception e;
        
        ResourceBrokerAdaptorTest resourceBrokerAdaptorTest;

        public JobStateMetricListener(
                ResourceBrokerAdaptorTest resourceBrokerAdaptorTest) {
            this.resourceBrokerAdaptorTest = resourceBrokerAdaptorTest;
        }

        public void processMetricEvent(MetricEvent val) {
            Job.JobState newState = (Job.JobState) val.getValue();
            if (newState.equals(Job.JobState.INITIAL)
                    && !(state.equals(Job.JobState.INITIAL))) {
                e = new Exception(newState + " occurs after " + state
                        + " but shouldn't");
            }
            if (newState.equals(Job.JobState.PRE_STAGING)
                    && (state.equals(Job.JobState.SCHEDULED)
                            || state.equals(Job.JobState.POST_STAGING)
                            || state.equals(Job.JobState.RUNNING)
                            || state.equals(Job.JobState.STOPPED) || state
                            .equals(Job.JobState.SUBMISSION_ERROR))) {
                e = new Exception(newState + " occurs after " + state
                        + " but shouldn't");
            }
            if (newState.equals(Job.JobState.SCHEDULED)
                    && (state.equals(Job.JobState.POST_STAGING)
                            || state.equals(Job.JobState.RUNNING)
                            || state.equals(Job.JobState.STOPPED) || state
                            .equals(Job.JobState.SUBMISSION_ERROR))) {
                e = new Exception(newState + " occurs after " + state
                        + " but shouldn't");
            }
            if (newState.equals(Job.JobState.RUNNING)
                    && (state.equals(Job.JobState.POST_STAGING)
                            || state.equals(Job.JobState.STOPPED) || state
                            .equals(Job.JobState.SUBMISSION_ERROR))) {
                e = new Exception(newState + " occurs after " + state
                        + " but shouldn't");
            }
            if (newState.equals(Job.JobState.POST_STAGING)
                    && (state.equals(Job.JobState.STOPPED) || state
                            .equals(Job.JobState.SUBMISSION_ERROR))) {
                e = new Exception(newState + " occurs after " + state
                        + " but shouldn't");
            }
            state = newState;
            if (state.equals(Job.JobState.STOPPED)) {
                synchronized (resourceBrokerAdaptorTest) {
                    resourceBrokerAdaptorTest.jobFinished = true;
                    resourceBrokerAdaptorTest.notifyAll();
                }
            }
        }

        public Exception getException() {
            return e;
        }
    }

    private AdaptorTestResultEntry submitJobGetInfo(GATContext gatContext,
            Preferences preferences, String host) {
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/sleep");
        sd.setArguments("2");
        JobDescription jd = new JobDescription(sd);
        ResourceBroker broker;
        try {
            broker = GAT.createResourceBroker(gatContext, preferences,
                    new URI(host));
        } catch (GATObjectCreationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        } catch (URISyntaxException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }
        Job job = null;
        Exception exception = null;
        long start = System.currentTimeMillis();
        try {
            job = broker.submitJob(jd);
        } catch (GATInvocationException e) {
            return new AdaptorTestResultEntry(false, 0L, e);
        }

        while (job.getState() != Job.JobState.STOPPED) {
            Map<String, Object> info = null;
            try {
                info = job.getInfo();
            } catch (GATInvocationException e) {
                exception = e;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    // ignored
                }
                continue;
            }
            if (info == null) {
                exception = new Exception("getInfo returns null");
            } else {
                if (!info.containsKey("state")) {
                    exception = new Exception(
                            "getInfo doesn't contain a key 'state'");
                }
                if (!info.containsKey("hostname")) {
                    exception = new Exception(
                            "getInfo doesn't contain a key 'hostname'");
                } else {
                    if (info.get("state").equals(Job.JobState.RUNNING)
                            && info.get("hostname") == null) {
                        exception = new Exception(
                                "inconsistent getInfo: state=RUNNING, hostname=null");
                    }
                }
                if (!info.containsKey("submissiontime")) {
                    exception = new Exception(
                            "getInfo doesn't contain a key 'submissiontime'");
                }
                if (!info.containsKey("starttime")) {
                    exception = new Exception(
                            "getInfo doesn't contain a key 'starttime'");
                }
                if (!info.containsKey("stoptime")) {
                    exception = new Exception(
                            "getInfo doesn't contain a key 'stoptime'");
                }
                if (!info.containsKey("poststage.exception")) {
                    exception = new Exception(
                            "getInfo doesn't contain a key 'poststage.exception'");
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                exception = e;
            }
        }

        long stop = System.currentTimeMillis();
        return new AdaptorTestResultEntry(exception == null, (stop - start),
                exception);

    }

    public void processMetricEvent(MetricEvent val) {
        if (val.getValue().equals(Job.JobState.STOPPED)) {
            synchronized (this) {
                jobFinished = true;
                notifyAll();
            }
        }
    }
}
