package tutorial20;

import javax.swing.JOptionPane;
import javax.swing.JPasswordField;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.File;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.SoftwareDescription;
import org.gridlab.gat.resources.Job.JobState;
import org.gridlab.gat.security.CertificateSecurityContext;

public class SubmitRemoteJob {
    
    private static String getPassphrase() {
        JPasswordField pwd = new JPasswordField();
        Object[] message = { "grid-proxy-init\nPlease enter your passphrase.",
                pwd };
        JOptionPane.showMessageDialog(null, message, "Grid-Proxy-Init",
                JOptionPane.QUESTION_MESSAGE);
        return new String(pwd.getPassword());
    }
    
    public static void main(String[] args) throws Exception {
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/hostname");
        File stdout = GAT.createFile("hostname.txt");
        sd.setStdout(stdout);

        JobDescription jd = new JobDescription(sd);
        
        Preferences prefs = new Preferences();
        /*
        prefs.put("VirtualOrganisation", "pvier");
        prefs.put("vomsServerURL", "voms.grid.sara.nl");
        prefs.put("vomsServerPort", "30000");
        prefs.put("vomsHostDN", "/O=dutchgrid/O=hosts/OU=sara.nl/CN=voms.grid.sara.nl");
        */
        
        CertificateSecurityContext ctxt = new CertificateSecurityContext(
                new URI(System.getProperty("user.home") + "/.globus/userkey.pem"),
                new URI(System.getProperty("user.home") + "/.globus/usercert.pem"),
                getPassphrase()
                );

        GATContext context = new GATContext();

        context.addPreferences(prefs);
        context.addSecurityContext(ctxt);
        ResourceBroker broker = GAT.createResourceBroker(context, new URI(args[0]));
        Job job = broker.submitJob(jd);

        while ((job.getState() != JobState.STOPPED)
                && (job.getState() != JobState.SUBMISSION_ERROR)) {
            System.out.println("State: " + job.getState());
            Thread.sleep(1000);
        }
    }
}
