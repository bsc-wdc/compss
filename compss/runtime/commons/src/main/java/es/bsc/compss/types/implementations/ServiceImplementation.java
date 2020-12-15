package es.bsc.compss.types.implementations;

import es.bsc.compss.types.implementations.definition.ServiceDefinition;
import es.bsc.compss.types.resources.ServiceResourceDescription;


public class ServiceImplementation extends Implementation {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Generate Dummy service implementation.
     * 
     * @return Dummy service implementation.
     */
    public static ServiceImplementation generateDummy() {
        return new ServiceImplementation(null, null, new ImplementationDescription<>(
            new ServiceDefinition("", "", "", ""), "", new ServiceResourceDescription("", "", "", 0)));
    }

    public ServiceImplementation() {
        super();
    }

    public ServiceImplementation(Integer coreId, Integer implId,
        ImplementationDescription<ServiceResourceDescription, ServiceDefinition> implDesc) {
        super(coreId, implId, implDesc);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ImplementationDescription<ServiceResourceDescription, ServiceDefinition> getDescription() {
        return (ImplementationDescription<ServiceResourceDescription, ServiceDefinition>) this.implDescription;

    }

    @Override
    public ServiceResourceDescription getRequirements() {
        return this.getDescription().getConstraints();

    }

    public String getOperation() {
        return getDescription().getDefinition().getOperation();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.SERVICE;
    }

}
