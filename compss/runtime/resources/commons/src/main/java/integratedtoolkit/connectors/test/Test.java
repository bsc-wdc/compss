package integratedtoolkit.connectors.test;

import integratedtoolkit.connectors.AbstractConnector;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Dummy connector for testing purposes
 * Needs to be inside main package (not test) because it is tested on execution time (constraintsTest)
 * 
 */
public class Test extends AbstractConnector {

    private static AtomicInteger nextId = new AtomicInteger(100);

    public Test(String providerName, HashMap<String, String> props) {
        super(providerName, props);
    }

    @Override
    public Object create(String name, CloudMethodResourceDescription rd) throws ConnectorException {
        TestEnvId envId = new TestEnvId();
        System.out.println("Es demana la creació de la màquina " + rd + "que correspon a " + envId);
        return envId;
    }

    @Override
    public CloudMethodResourceDescription waitUntilCreation(Object vm, CloudMethodResourceDescription requested) throws ConnectorException {
        try {
            Thread.sleep(15000);
        } catch(Exception e) {
        	// No need to handle such exception
        }
        
        System.out.println("Waiting for VM creation " + vm);
        CloudMethodResourceDescription granted = new CloudMethodResourceDescription(requested);
        granted.setName("127.0.0." + nextId.getAndIncrement());
        granted.setOperatingSystemType(requested.getImage().getOperatingSystemType());
        
        return granted;
    }

    @Override
    public float getMachineCostPerTimeSlot(CloudMethodResourceDescription rd) {
        return 0.0f;
    }

    @Override
    public long getTimeSlot() {
        return ONE_HOUR;
    }

    @Override
    public void destroy(Object envId) throws ConnectorException {
        System.out.println("S'esta destruint " + envId);
    }

    @Override
    public void configureAccess(String IP, String user, String password) throws ConnectorException {
        System.out.println("Otorgant accés al master a la màquina "+IP);
    }

    @Override
    public void prepareMachine(String IP, CloudImageDescription cid) throws ConnectorException {
        System.out.println("Copiant tota la informació necessaria a "+IP);
    }

    private static class TestEnvId {

        private static AtomicInteger nextId = new AtomicInteger(0);
        private int id = nextId.getAndIncrement();

        public String toString() {
            return "TestEventId:" + id;
        }
    }

	@Override
	protected void close() {
		// Nothing to do
	}
	
}
