package integratedtoolkit.types.resources;

import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;


public class MethodWorker extends Worker<MethodResourceDescription> {

    private String name;

    // Available resource capabilities
    protected final MethodResourceDescription available;

    
    public MethodWorker(String name, MethodResourceDescription description, COMPSsWorker worker) {
        super(name, description, worker);

        this.name = name;
        available = new MethodResourceDescription(description);
    }

    public MethodWorker(String name, MethodResourceDescription description, MethodConfiguration config) throws Exception {
        super(name, description, config);
        
        this.name = name;
        this.available = new MethodResourceDescription(description); // clone
    }

    public MethodWorker(MethodWorker mw) {
        super(mw);
        this.available = mw.available.copy();
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean reserveResource(MethodResourceDescription consumption) {
    	synchronized(available) {
    		available.reduce(consumption);
    	}
        
        return true;
    }

    @Override
    public void releaseResource(MethodResourceDescription consumption) {
    	synchronized(available) {
    		available.increase(consumption);
    	}
    }

    @Override
    public boolean hasAvailable(MethodResourceDescription consumption) {
    	synchronized(available) {
    		return ((available.getTotalComputingUnits() >= consumption.getTotalComputingUnits())
    				&& (available.getMemorySize() >= consumption.getMemorySize()));
    	}
    }

    @Override
    public Integer fitCount(Implementation<?> impl) {
        if (impl.getType() == Implementation.Type.SERVICE) {
            return null;
        }
        MethodResourceDescription ctrs = (MethodResourceDescription) impl.getRequirements();
        return description.canHostSimultaneously(ctrs);
    }

    @Override
    public Type getType() {
        return Type.WORKER;
    }

    @Override
    public String getMonitoringData(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("<TotalComputingUnits>").append(description.getTotalComputingUnits()).append("</TotalComputingUnits>").append("\n");
        sb.append(prefix).append("<Memory>").append(description.getMemorySize()).append("</Memory>").append("\n");
        sb.append(prefix).append("<Disk>").append(description.getStorageSize()).append("</Disk>").append("\n");
        return sb.toString();
    }

    private Float getValue() {
        return description.value;
    }

    @Override
    public int compareTo(Resource t) {
        if (t == null) {
            throw new NullPointerException();
        }
        switch (t.getType()) {
            case SERVICE:
                return 1;
            case WORKER:
                MethodWorker w = (MethodWorker) t;
                if (description.getValue() == null) {
                    if (w.getValue() == null) {
                        return w.getName().compareTo(getName());
                    }
                    return 1;
                }
                if (w.getValue() == null) {
                    return -1;
                }
                float dif = w.getValue() - description.getValue();
                if (dif > 0) {
                    return -1;
                }
                if (dif < 0) {
                    return 1;
                }
                return getName().compareTo(w.getName());
            case MASTER:
                return -1;
            default:
                return getName().compareTo(t.getName());
        }
    }

    @Override
    public boolean canRun(Implementation<?> implementation) {
        switch (implementation.getType()) {
            case METHOD:
                MethodResourceDescription ctrs = (MethodResourceDescription) implementation.getRequirements();
                return description.contains(ctrs);
            default:
                return false;
        }
    }

    @Override
    public String getResourceLinks(String prefix) {
        StringBuilder sb = new StringBuilder(super.getResourceLinks(prefix));
        sb.append(prefix).append("TYPE = WORKER").append("\n");
        sb.append(prefix).append("COMPUTING_UNITS = ").append(description.getTotalComputingUnits()).append("\n");
        sb.append(prefix).append("MEMORY = ").append(description.getMemorySize()).append("\n");

        return sb.toString();
    }

    @Override
    public Worker<?> getSchedulingCopy() {
        return new MethodWorker(this);
    }

}
