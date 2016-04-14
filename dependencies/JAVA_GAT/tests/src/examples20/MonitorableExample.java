package examples20;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.monitoring.MetricDefinition;
import org.gridlab.gat.monitoring.Monitorable;

public class MonitorableExample {

    /**
     * This example shows the use of Monitorables in JavaGAT.
     * 
     * It prints all the metric definitions that are provided by the
     * Monitorable. In order to run this example using Mercury, you've to run it
     * on a machine that has Mercury installed. Please put this
     * /path/to/mercury-monitor-2.3.1/lib in your $LD_LIBRARY_PATH.
     * 
     * 
     * @param args
     * @throws GATInvocationException
     * @throws GATObjectCreationException
     */
    public static void main(String[] args) throws GATObjectCreationException,
            GATInvocationException {
        new MonitorableExample().start();
        GAT.end();
    }

    public void start() throws GATObjectCreationException,
            GATInvocationException {
        Monitorable monitorable = GAT.createMonitorable();
        for (MetricDefinition m : monitorable.getMetricDefinitions()) {
            System.out.println(m);
        }

    }
}
