/*
 * Created on Aug 16, 2004
 */
package advert;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.URI;
import org.gridlab.gat.advert.AdvertService;
import org.gridlab.gat.advert.MetaData;
import org.gridlab.gat.io.File;
import org.gridlab.gat.resources.HardwareResourceDescription;
import org.gridlab.gat.resources.Job;
import org.gridlab.gat.resources.JobDescription;
import org.gridlab.gat.resources.ResourceBroker;
import org.gridlab.gat.resources.ResourceDescription;
import org.gridlab.gat.resources.SoftwareDescription;

/**
 * @author rob
 */
public class AdvertJob {
    public static void main(String[] args) throws Exception {
        try {
        GATContext c = new GATContext();
        Preferences prefs = new Preferences();
        prefs.put("File.adaptor.name", "local,commandlinessh");
        prefs.put("job.stop.on.exit", "false");
        c.addPreferences(prefs);
        
        SoftwareDescription sd = new SoftwareDescription();
        sd.setExecutable("/bin/sleep");
        sd.setArguments("100");
 
        // stdout & stderr
        File stdout = GAT.createFile(c, "std.out");
        File stderr = GAT.createFile(c, "std.err");
        sd.setStderr(stderr);
        sd.setStdout(stdout);

        ResourceDescription rd = new HardwareResourceDescription();

        JobDescription jd = new JobDescription(sd, rd);
        ResourceBroker broker = GAT.createResourceBroker(c, new URI("sshsge://fs0.das3.cs.vu.nl"));
        Job job = broker.submitJob(jd);

        AdvertService a = GAT.createAdvertService(c);
        MetaData m = new MetaData();
        m.put("name", "testJob");
        a.add(job, m, "/rob/testJob");
        a.exportDataBase(new URI("file:///mydb"));

        GAT.end();
        System.exit(0);
        } catch(Throwable e) {
            e.printStackTrace();
        }
    }
}
