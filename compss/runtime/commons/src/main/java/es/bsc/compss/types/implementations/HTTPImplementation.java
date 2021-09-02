package es.bsc.compss.types.implementations;

import es.bsc.compss.types.implementations.definition.HTTPDefinition;
import es.bsc.compss.types.resources.HTTPResourceDescription;

import java.util.ArrayList;


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
        return new HTTPImplementation(null, null, new ImplementationDescription<>(
            new HTTPDefinition("", "", "", "", "", ""), "", new HTTPResourceDescription(new ArrayList<String>(), 0)));
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

    public String getRequest() {
        return getDescription().getDefinition().getRequest();
    }

    public String getResource() {
        return getDescription().getDefinition().getResource();
    }

    public String getPayload() {
        return getDescription().getDefinition().getPayload();
    }

    public String getPayloadType() {
        return getDescription().getDefinition().getPayloadType();
    }

    public String getProduces() {
        return getDescription().getDefinition().getProduces();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.HTTP;
    }
}
