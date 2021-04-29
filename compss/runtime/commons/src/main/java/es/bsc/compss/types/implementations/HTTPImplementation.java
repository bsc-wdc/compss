package es.bsc.compss.types.implementations;

import es.bsc.compss.types.implementations.definition.HTTPDefinition;
import es.bsc.compss.types.resources.HTTPResourceDescription;


public class HTTPImplementation extends Implementation {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Generate Dummy HTTP implementation.
     *
     * @return Dummy HTTP implementation.
     */
    public static HTTPImplementation generateDummy() {
        return new HTTPImplementation(null, null,
            new ImplementationDescription<>(new HTTPDefinition("", ""), "", new HTTPResourceDescription(0)));
    }

    public HTTPImplementation() {
        super();
    }

    public HTTPImplementation(Integer coreId, Integer implId,
        ImplementationDescription<HTTPResourceDescription, HTTPDefinition> implDesc) {

        super(coreId, implId, implDesc);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ImplementationDescription<HTTPResourceDescription, HTTPDefinition> getDescription() {
        return (ImplementationDescription<HTTPResourceDescription, HTTPDefinition>) this.implDescription;
    }

    @Override
    public HTTPResourceDescription getRequirements() {
        return this.getDescription().getConstraints();
    }

    public String getMethodType() {
        return getDescription().getDefinition().getMethodType();
    }

    public String getBaseUrl() {
        return getDescription().getDefinition().getBaseUrl();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.HTTP;
    }
}
