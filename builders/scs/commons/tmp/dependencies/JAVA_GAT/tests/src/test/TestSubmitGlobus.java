package test;

import javax.swing.JOptionPane;
import javax.swing.JPasswordField;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.File;
import org.gridlab.gat.monitoring.MetricEvent;
import org.gridlab.gat.monitoring.MetricListener;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.security.CertificateSecurityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSubmitGlobus implements MetricListener {
    
    public static final int NFILES = 10;
    public static final int NJOBS = 10;
    public static final int NBATCHES = 100;
    public static final int BACK = 3;           // Max BACK * NJOBS running simultaneously.
    
    private static Logger logger = LoggerFactory
            .getLogger(TestSubmitGlobus.class);
    
    private static String getPassphrase() {
        JPasswordField pwd = new JPasswordField();
        Object[] message = { "grid-proxy-init\nPlease enter your passphrase.",
                pwd };
        JOptionPane.showMessageDialog(null, message, "Grid-Proxy-Init",
                JOptionPane.QUESTION_MESSAGE);
        return new String(pwd.getPassword());
    }
    
    public static void main(String[] args) throws Exception {
	
	TestSubmitGlobus g = new TestSubmitGlobus();
	g.test(args[0]);
    }

    private int finished;
    
    public void test(final String host) throws Exception {
	
        final SoftwareDescription sd = new SoftwareDescription();
        
        sd.setExecutable("/bin/sleep");
        sd.setArguments("10");

        final JobDescription jd = new JobDescription(sd);
        
        Preferences prefs = new Preferences();
        prefs.put("resourcebroker.adaptor.name", "wsgt4new");
        prefs.put("file.adaptor.name", "local,gridftp");
        
        CertificateSecurityContext ctxt = new CertificateSecurityContext(
                new URI(System.getProperty("user.home") + "/.globus/userkey.pem"),
                new URI(System.getProperty("user.home") + "/.globus/usercert.pem"),
                getPassphrase()
                );

        final GATContext context = new GATContext();

        context.addPreferences(prefs);
        context.addSecurityContext(ctxt);
        
        File[] files = new File[NFILES];
        for (int i = 0; i < files.length; i++) {
            files[i] = GAT.createFile(context, "file" + i);
            files[i].createNewFile();
        }
        for (File f : files) {
            sd.addPreStagedFile(f);
            sd.addPostStagedFile(f);
        }

        final ResourceBroker broker = GAT.createResourceBroker(context, new URI(host));           
        for (int j = 0; j < NBATCHES; j++) {
            logger.info("Starting a batch of " + NJOBS + " jobs");
            for (int i = 0; i < NJOBS; i++) {
                Thread t = new Thread() {
                    public void run() {
                        try {
                            broker.submitJob(jd, TestSubmitGlobus.this, "job.status");
                        } catch(Throwable e) {
                            System.out.println("Submit failed");
                            e.printStackTrace();
                            synchronized(TestSubmitGlobus.this) {
                                finished++;
                                TestSubmitGlobus.this.notifyAll();
                            }
                        }
                    }
                };
                t.start();
            }
            synchronized(this) {
                while (finished < NJOBS * (j-BACK)) {
                    logger.info("Waiting until at least " + (NJOBS * (j-BACK)) + " jobs are finished");
                    try {
                        wait();
                    } catch(Throwable e) {
                        // ignore
                    }
                }
            }
        }

        synchronized(this) {
            while (finished != NJOBS * NBATCHES) {
        	try {
        	    wait();
        	} catch(Throwable e) {
        	    // ignore
        	}
            }
        }
    }

    @Override
    public void processMetricEvent(MetricEvent event) {
        if (event.getValue().equals(Job.JobState.STOPPED) || event.getValue().equals(Job.JobState.SUBMISSION_ERROR)) {
            synchronized (this) {
                finished++;
                notifyAll();
            }
        }
    }
}
