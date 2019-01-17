/*
 * Created on Aug 16, 2004
 */
package advert;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.Preferences;
import org.gridlab.gat.URI;
import org.gridlab.gat.advert.AdvertService;
import org.gridlab.gat.resources.Job;

/**
 * @author rob
 */
public class ConnectToAdvertJob {
    public static void main(String[] args) throws Exception {
        GATContext c = new GATContext();
        Preferences prefs = new Preferences();
        prefs.put("File.adaptor.name", "local");
        
        AdvertService a = GAT.createAdvertService(c, prefs);
        
        a.importDataBase(new URI("file:///mydb"));
        c = new GATContext();
        prefs = new Preferences();
        // prefs.put("Resourcebroker.adaptor.name", "sshsge");
        prefs.put("File.adaptor.name", "local, commandlinessh");
        c.addPreferences(prefs);
        Job other = (Job) a.getAdvertisable(c, "/rob/testJob");

        System.err.println("got job back: " + other);

        while ((other.getState() != Job.JobState.STOPPED)
                && (other.getState() != Job.JobState.SUBMISSION_ERROR)) {
            System.err.println("job state = " + other.getInfo());
            Thread.sleep(1000);
        }

        System.err.println("job DONE, state = " + other.getInfo());
        GAT.end();
    }
}
